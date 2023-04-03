import os.path
from pathlib import Path
import sys
import re
import html
import time
import shutil

import sqlite3
from datetime import datetime, timedelta
import json
import subprocess

import feedparser
from mastodon import Mastodon
from mastodon.Mastodon import MastodonAPIError, MastodonBadGatewayError
import requests

from decimal import *



############################################################################################
# Configuration

# Maximum video size.
kMAX_VIDEO_SIZE_MB = 40

# App name.
kAPP_NAME = 'tootbot'



############################################################################################
# Helpers

# Resolve redirected links.
def unredir(redir):
    r = requests.get(redir, allow_redirects = False)

    redir_count = 0

    while r.status_code in { 301, 302 }:
        redir_count = redir_count + 1

        if redir_count > 10:
            break

        if 'http' not in r.headers.get('Location'):
            redir = re.sub(r'(https?://.*)/.*', r'\1', redir) + r.headers.get('Location')
        else:
            redir = r.headers.get('Location')

        if '//ow.ly/' in redir or '//bit.ly/' in redir:
            redir = redir.replace('https://ow.ly/', 'http://ow.ly/') # only http
            redir = requests.get(redir, allow_redirects = False).headers.get('Location')

        try:
            r = requests.get(redir, allow_redirects = False, timeout = 5)
        except:
            redir = redir.replace('https://', 'http://')  # only http ?
            r = requests.get(redir, allow_redirects = False)

    return redir

# Remove a file and ignore errors.
def unlink_path(file_path):
    try:
        file_path.unlink();
    except:
        pass

# Post a media to Mastodon.
def mastodon_media_post(mastodon_api, data, mime_type, updater = None):

    # Updater helper.
    def lupdater(text):
        if updater is not None:
            updater(text)

    # Prepate things.
    try_count = 0

    # Re-try loop.
    while True:
        try:
            media_posted = mastodon_api.media_post(data, mime_type = mime_type)

            return media_posted['id']
                        
        except MastodonBadGatewayError as e:
            try_count = try_count + 1
                    
            if try_count >= 10:
                raise
            else:
                lupdater('unable to send media, will retry in 10 seconds (' + str(e) + ')')
                time.sleep(10)
                        
        except Exception as e:
            raise
        
# Post a toot to mastodon.
def mastodon_post(mastodon_api, tweet_content, photos_ids, videos_ids, updater = None):

    # Updater helper.
    def lupdater(text):
        if updater is not None:
            updater(text)

    # Prepare things.
    try_count = 0
    medias_ids = photos_ids + videos_ids

    # Wait a bit for media to be ready on server side.
    if len(medias_ids) > 0:
        time.sleep(5)

    # Re-try loop.
    while True:
        try:
            toot = mastodon_api.status_post(tweet_content,
                                            in_reply_to_id = None,
                                            media_ids = medias_ids,
                                            sensitive = False,
                                            visibility = 'unlisted',
                                            spoiler_text = None)
            
            return toot
        
        except MastodonAPIError as e:
            description = str(e).lower()
            try_count = try_count + 1

            # Specific error catching work only with English instances. Not sure why Mastodon localize that.
            if '422' in description and 'Unprocessable Entity'.lower() in description and 'Try again in a moment'.lower() in description:
                if try_count >= 10:
                    raise Exception('Medias take too long to proceed')
                else:
                    lupdater('medias are still processing, will retry in 10 seconds (' + str(e) + ')')
                    time.sleep(10)

            elif '422' in description and 'Unprocessable Entity'.lower() in description and 'Cannot attach a video to a post that already contains images'.lower() in description:
                if try_count >= 2:
                    raise Exception('Unexpected mixed images and videos')
                else:
                    lupdater('mixed images and videos, will retry in 5 seconds with only videos (' + str(e) + ')')
                    medias_ids = videos_ids
                    time.sleep(5)
                    
            else:
                if try_count >= 5:
                    raise Exception('Got an unknown API error - ' + str(e))
                else:
                    lupdater('got an unknown API error, will retry in 10 seconds (' + str(e) + ')')
                    time.sleep(10)

        except Exception as e:
            raise

# Download and recompress a video.
def download_video(video_url, video_path, max_video_size_mb, updater = None):

    # Updater helper.
    def lupdater(text):
        if updater is not None:
            updater(text)
    
    # Remove file, in case the directory is dirty.
    unlink_path(video_path)

    # Download video.
    lupdater("downloading the video")

    try:
        subprocess.run("yt-dlp -o '%s' -N 8 -f b -S 'filesize~%sM' --recode-video mp4 --no-playlist --max-filesize 500M '%s'" %
                        (str(video_path), str(max_video_size_mb), video_url), shell = True, capture_output = False, check = True)
    except Exception as e:
        unlink_path(video_path)
        raise
    
    lupdater("video downloaded")

    # Recompress
    try:
        tmp_video_path = video_path.with_name('tmp-' + video_path.name)
        pass_video_path_prefix = video_path.with_name(video_path.name + '-ffmpeg2pass')

        size = os.lstat(video_path).st_size
                    
        if size > max_video_size_mb * 1024 * 1024:
            lupdater('video too big (%s > %s), recompressing' % (size, max_video_size_mb * 1024 * 1024))
            
            # > Compute the needed bitrate.
            duration_result = subprocess.run("ffprobe -v error -show_entries format=duration -of csv=p=0 '%s'"
                                             % (str(video_path)),
                                             shell = True, capture_output = True, text = True, check = True)
            
            audio_bitrate_result = subprocess.run("ffprobe -v error -select_streams a:0 -show_entries stream=bit_rate -of csv=p=0 '%s'"
                                                  % (str(video_path)),
                                                  shell = True, capture_output=True, text = True, check = True)

            duration_dec = Decimal(duration_result.stdout)
            audio_bitrate_dec = Decimal(audio_bitrate_result.stdout)
                        
            if audio_bitrate_dec > Decimal(128000):
                audio_bitrate_dec = Decimal(128000)
                        
            target_audio_bitrate_kbit_s = audio_bitrate_dec / Decimal(1000.0)
            target_video_bitrate_kbit_s = (Decimal(max_video_size_mb) * Decimal(8192.0)) / (Decimal(1.048576) * duration_dec) - target_audio_bitrate_kbit_s
                        
            if target_video_bitrate_kbit_s <= Decimal(0):
                raise Exception('result in negative bitrate ', target_video_bitrate_kbit_s)

            # > Remove previous pass files, in case the directory is dirty.
            for path in pass_video_path_prefix.parent.glob(pass_video_path_prefix.name + '*'):
                unlink_path(path)

            # > Encode the video in 2 passes, for better size accuracy.
            subprocess.run("ffmpeg -y -i '%s' -c:v libx264 -b:v %sk -pass 1 -an -f mp4 -passlogfile '%s' -loglevel error -stats /dev/null" %
                           (str(video_path), str(target_video_bitrate_kbit_s), str(pass_video_path_prefix)),
                           shell = True, capture_output = False, check = True)
            
            subprocess.run("ffmpeg -y -i '%s' -c:v libx264 -b:v %sk -pass 2 -c:a aac -b:a %sk -passlogfile '%s' -loglevel error -stats '%s'"
                           % (str(video_path), str(target_video_bitrate_kbit_s), str(target_audio_bitrate_kbit_s), str(pass_video_path_prefix), str(tmp_video_path)),
                           shell = True, capture_output = False, check = True)
            
            # Move files.
            video_path.unlink()
            tmp_video_path.rename(video_path)

    except Exception as e:
            lupdater("unable to recompress video (" + str(e) + ")")

    finally:
        # > Remove pass files, in case the directory is dirty.
        for path in pass_video_path_prefix.parent.glob(pass_video_path_prefix.name + '*'):
            unlink_path(path)
        
        # > Remove temp file.
        unlink_path(tmp_video_path)



############################################################################################
# Main

root_path = Path()

# Check arguments.
if len(sys.argv) < 5:
    print("Usage: python3 tootbot.py twitter_account mastodon_login mastodon_passwd mastodon_instance [max_days [footer_tags [delay]]]")
    sys.exit(1)

# Extract arguments.
twitter_account = sys.argv[1]
mastodon_login = sys.argv[2]
mastodon_passwd = sys.argv[3]
mastodon_instance = sys.argv[4]

if len(sys.argv) > 5:
    max_days = int(sys.argv[5])
else:
    max_days = 1

if len(sys.argv) > 6:
    footer_tags = sys.argv[6]
else:
    footer_tags = None

if len(sys.argv) > 7:
    delay = int(sys.argv[7])
else:
    delay = 0

# Forge log prefix.
log_prefix = twitter_account + ':'

# Create directory for Twitter account.
account_path = root_path.joinpath(twitter_account)

if account_path.exists():
    if not account_path.is_dir():
        print(log_prefix, 'Cannot create directory "' + str(account_path) + '" because a file with this name altready exists.')
        sys.exit(1)
else:
    try:
        os.mkdir(twitter_account)
    except Exception as e:
        print(log_prefix, 'Cannot create directory "'+ str(account_path) + '":', e)
        sys.exit(1)


# Open database.
sql_path = account_path.joinpath('tootbot.db')

try:
    sql = sqlite3.connect(sql_path)
    db = sql.cursor()
    db.execute('''CREATE TABLE IF NOT EXISTS tweets (tweet text, toot text, twitter text, mastodon text, instance text)''')
except Exception as e:
    print(log_prefix, 'Cannot create database file "' + str(sql_path) + '":', e)
    sys.exit(1)

# Create application if it does not exist.
mastodon_secret_path = account_path.joinpath(mastodon_instance + '.secret')
mastodon_base_url = 'https://' + mastodon_instance

if not mastodon_secret_path.exists():
    if Mastodon.create_app(kAPP_NAME, api_base_url = mastodon_base_url, to_file = mastodon_secret_path):
        print(log_prefix, 'Tootbot app created on instance "' + mastodon_instance + '".')
    else:
        print(log_prefix, 'Failed to create app on instance "' + mastodon_instance + '".')
        sys.exit(1)


# Login to Mastodon.
print(log_prefix, 'Login to Mastodon "' + mastodon_login + '".')

login_secret_path = account_path.joinpath(mastodon_login + '.secret')

try:
    mastodon_api = Mastodon(client_id = mastodon_secret_path, api_base_url = mastodon_base_url)

    mastodon_api.log_in(
        username = mastodon_login,
        password = mastodon_passwd,
        scopes = ['read', 'write'],
        to_file = login_secret_path
    )
except Exception as e:
    print(log_prefix, 'Login failed -', e)
    sys.exit(1)

# Remove previous fetched tweets.
for path in account_path.glob('tweets.*json'):
    unlink_path(path)


# Fetch tweets.
print(log_prefix, 'Fetching tweets.')
      
twitter_sjson_path = account_path.joinpath('tweets.sjson')
twitter_json_path = account_path.joinpath('tweets.json')

try:
    subprocess.run("twint -u '%s' -tl --full-text --limit 10 --json -o '%s'" % (twitter_account, str(twitter_sjson_path),), shell = True, capture_output = True, check = True)
    subprocess.run("jq -s . '%s' > '%s'" % (str(twitter_sjson_path), str(twitter_json_path),), shell = True, capture_output = True, check = True)
except Exception as e:
    print(log_prefix, 'Failed to fetch tweets -', e)
    sys.exit(1)


# Load JSON.
try:
    tweets = json.load(open(twitter_json_path, 'r'))
except Exception as e:
    print(log_prefix, 'Failed to parse tweets -', e)
    sys.exit(1)

print(log_prefix, 'Fetched', len(tweets), 'tweets.')

for tweet in reversed(tweets):
    tweet_content_raw =  tweet['tweet']
    tweet_content = html.unescape(tweet_content_raw)
    tweet_id = tweet['id']
    tweet_username = tweet['username']
    tweet_photos_url = []

    toot_photos_ids = []
    toot_videos_ids = []

    # Define log helper.
    def log_updater(text):
        print(log_prefix, text.capitalize() + '.')

    # Do not toot twitter replies.
    if 'reply_to' in tweet and len(tweet['reply_to'])>0:
        print(log_prefix, 'Reply - "' + tweet_content.replace('\n', ' ') + '".')
        continue
    
    # Do not toot twitter quoted RT. XXX Perhaps reconsider that.
    if 'quote_url' in tweet and tweet['quote_url'] != '':
        print(log_prefix, 'Quoted - "' + tweet_content.replace('\n', ' ') + '".')
        continue

    # Skip invalid content.
    if tweet_content[-1] == "…":
        continue

    # Check if this tweet has been processed
    try:
        db.execute('SELECT * FROM tweets WHERE tweet = ? AND twitter = ? and mastodon = ? and instance = ?', (tweet_id, twitter_account, mastodon_login, mastodon_instance))  # noqa
        last = db.fetchone()

        if last:
            continue
    except Exception as e:
        print(log_prefix, 'Cannot check if tweet', tweet_id, 'exist in database -', e)
        continue

    print('Tweet - "' + tweet_content.replace('\n', ' ') + '"')

    # Handle retweet.
    if twitter_account and tweet_username.lower() != twitter_account.lower():
        tweet_content = ("RT https://twitter.com/%s\n" % tweet_username) + tweet_content

        # > Extract photo URLs. XXX not sure why, they are not part of `tweet['photos']` in such case ?
        for photo in re.finditer(r"https://pbs.twimg.com/[^ \xa0\"]*", tweet_content_raw):
            photo_url = photo.group(0)
            tweet_photos_url.append(photo_url)

    # Handle photos.
    if 'photos' in tweet:
        tweet_photos_url = tweet_photos_url + tweet['photos']

    for photo_url in tweet['photos']:
        media = None

        print(log_prefix, 'Download photo "' + photo_url + '".')

        # > Try by passing by nitter.
        if media is None:
            try:
                media = requests.get(photo_url.replace('https://pbs.twimg.com/', 'https://nitter.net/pic/orig/'))
            except Exception as e:
                print(log_prefix, 'Failed to download the photo via nitter -', e)

        # > Try by using the original link.
        if media is None:
            try:
                media = requests.get(photo_url)
            except Exception as e:
                print(log_prefix, 'Failed to download the photo via original url -', e)

        # > Post.
        if media is not None:
            try:
                print(log_prefix, 'Upload photo to Mastodon server.')

                media_id = mastodon_media_post(mastodon_api, media.content, media.headers.get('content-type'), log_updater)

                toot_photos_ids.append(media_id)

                print(log_prefix, 'Uploaded photo (' + str(media_id) + ').')
            except Exception as e:
                print(log_prefix, 'Cannot upload photo -', e)

    
    # Handle inline links.
    links = re.findall(r"http[^ \xa0]*", tweet_content)
    
    for link in links:
        dir_link = unredir(link)
        link_handled = False

        # > Photo link: remove (they have been handled and attached in previous section).
        m = re.search(r'twitter.com/.*/photo/', dir_link)

        if m is not None:
            tweet_content = tweet_content.replace(link, '')
            link_handled = True

        # > Pic link: remove (they have been handled and attached in previous section).
        m = re.search(r"pic.twitter.com", link)

        if m is not None:
            tweet_content = tweet_content.replace(link, ' ')
            link_handled = True

        # > Video link: download, post, and remove the link.
        m = re.search(r'(twitter.com/.*/video/)', dir_link)
        
        if m is not None:
            video_path = account_path.joinpath('video.mp4')

            print(log_prefix, 'Download video "' + dir_link +  '".')

            try:
                # > Download the video.
                download_video(dir_link, video_path, kMAX_VIDEO_SIZE_MB, log_updater)

                # > Read video content.
                file = open(video_path, "rb")
                video_data = file.read()
                file.close()

                # > Post the video.
                print(log_prefix, 'Upload video to Mastodon server.')

                media_id = mastodon_media_post(mastodon_api, video_data, 'video/mp4', log_updater)

                print(log_prefix, 'Uploaded video (' + str(media_id) + ').')

                # > Store media id.
                toot_videos_ids.append(media_id)

                # > Remove the link.
                tweet_content = tweet_content.replace(link, '')
                link_handled = True

            except Exception as e:
                print(log_prefix, 'Cannot upload video -', e)


        # > Link not handled: replace with direct link.
        if link_handled == False:
            tweet_content = tweet_content.replace(link, dir_link)

    # Remove ellipsis
    tweet_content = tweet_content.replace('\xa0…', ' ')

    #c = c.replace('  ', '\n').replace('. ', '.\n')

    # Replace links to twitter by nitter ones.
    tweet_content = tweet_content.replace('/twitter.com/', '/nitter.net/')

    # Replace utm_? tracking.
    tweet_content = re.sub('\?utm.*$', '?utm_medium=Social&utm_source=Mastodon', tweet_content)

    if footer_tags:
        tweet_content = tweet_content + '\n' + footer_tags
    
    # Post.
    print(log_prefix, 'Posting toot.')

    try:
        # > Post the toot.
        toot = mastodon_post(mastodon_api, tweet_content, toot_photos_ids, toot_videos_ids, log_updater)

        # > Save in database.
        db.execute("INSERT INTO tweets VALUES ( ? , ? , ? , ? , ? )", (tweet_id, toot["id"], twitter_account, mastodon_login, mastodon_instance))
        sql.commit()
        
        # > Log post.
        print(log_prefix, 'Tweet created at', tweet['created_at'], "posted.")

    except Exception as e:
        print(log_prefix, e, '- skip tweet.')
