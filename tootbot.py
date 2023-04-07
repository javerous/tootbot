#! /usr/bin/env python3

import os.path
from pathlib import Path, PurePath
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
from mastodon.Mastodon import MastodonAPIError, MastodonBadGatewayError, MastodonInternalServerError
import requests

from decimal import *

from urllib.parse import urlparse, urlunparse, urlsplit, urlunsplit


'''
Note:
- https://github.com/twintproject/twint (Twint) have been archived for some obscure reason.
- https://github.com/twintproject/twint-zero (Twint-Zero) Seems that the designated successor.

It will probably require to switch to it at some point.

It has some advantages (simple interface, direct JSON output on stdout, etc.), but is too limited for
  now to use it as a replacement (some links are missing, like videos one, there is no conversation_id, no quote URL...)

Let stay on Twint for now.
'''



############################################################################################
# Configuration

# App name.
kAPP_NAME = 'tootbot'



############################################################################################
# Helpers

# Resolve redirected links.
# Note: if 'https' time-out, we retry with 'http'. It's unsafe, and in ideal it shouldn't be necessary,
#   but there is a combination of issues for some redirect links which force us to do it:
#     - Redirection like 't.co' may give an invalid https 301 / 'Location' (it doesn't generate this 301 / 'Location' response
#           when the 'User-Agent' looks like a browser: instead, an HTML page is generated which redirect to the actual proper URL).
#     - Redirection like 'u.afp.com' doesn't answer at all if accessed via 'https' / 443. They work only on 'http' / 80 (??!!).
#   So we have to try 'http'.

#   An alternative solution would be to do the same as browsers:
#     - Pass a browser-like 'User-Agent'.
#     - Use a 'GET' request instead of 'HEAD'.
#     - Parse the resulting HTML content, and try to catch things like 'http-equiv="refresh"', 'location' JavaScript, etc.
#   It's probably too much for what we want to achieve here, so we stay on an imperfect solution.
def unredir(redir):
    for redir_nbr in range(10):
        try:
            r = requests.head(redir, allow_redirects = False, timeout = 5)
            
            status_code = r.status_code
            location = r.headers.get('Location')
                
        except (requests.exceptions.ConnectTimeout, requests.exceptions.ReadTimeout) as e:
            redirs = urlsplit(redir)
            redir_scheme = redirs[0]

            if redir_scheme.lower() == 'https':
                redir = urlunsplit(('http', redirs[1], redirs[2], redirs[3], redirs[4]))
                continue
            
            return redir
        
        except Exception as e:
            return redir

        if status_code not in { 301, 302 }:
            return redir

        if 'http' not in location:
            redir = re.sub(r'(https?://.*)/.*', r'\1', redir) + location
        else:
            redir = location

    return redir

# Remove a file and ignore errors.
def unlink_noerr(file_path):
    try:
        file_path.unlink();
    except:
        pass

# Return an integer from int or str.
def safe_int(value):

    if value is None:
        return None

    if isinstance(value, int):
        return value
    
    try:
        return int(value)
    except:
        return -1
    
# Safely fetch an object in a hierarchy of objects.
def safe_dict(dict, path, default = None):
    parts = path.split('.')
    value = dict

    try:
        for part in parts:
            value = value[part]
    except:
        return default
    
    return value

# Replace substrings of a string if it don't make it exceed some length.
def safe_replace(string, oldvalue, newvalue, max_length, logger = None):

    # Logger helper.
    def llogger(*args):
        if logger is not None:
            logger(*args)

    # Replace
    result = string.replace(oldvalue, newvalue)

    # Check result.
    if len(result) > max_length:
        llogger('can\'t replace "', oldvalue, '" with "', newvalue, '", result is too long - ', len(result), ' > ', max_length)
        return string
    
    return result

# Concatenate string representation of random objets.
def stringify(*args):
    result = ''

    for arg in args:
        result += str(arg)

    return result
    
# Post a media to Mastodon. Return int media id.
def mastodon_media_post(mastodon_api, data, mime_type, logger = None):

    # Logger helper.
    def llogger(*args):
        if logger is not None:
            logger(*args)

    # Prepate things.
    try_count = 0

    # Re-try loop.
    while True:
        try_count = try_count + 1

        try:
            media_posted = mastodon_api.media_post(data, mime_type = mime_type)

            return safe_int(media_posted['id'])
                        
        except MastodonBadGatewayError as e:
            if try_count >= 10:
                raise
            else:
                llogger('unable to send media, will retry in 10 seconds - ', e)
                time.sleep(10)

        except MastodonInternalServerError as e:
            if try_count >= 5:
                raise
            else:
                llogger('unable to send media, will retry in 10 seconds - ', e)
                time.sleep(10)
                        
        except Exception as e:
            raise
        
# Post a toot to mastodon. Return toot dictionary.
def mastodon_post(mastodon_api, tweet_content, in_reply_to_id, photos_ids, videos_ids, logger = None):

    # Logger helper.
    def llogger(*args):
        if logger is not None:
            logger(*args)

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
                                            in_reply_to_id = in_reply_to_id,
                                            media_ids = medias_ids,
                                            sensitive = False,
                                            visibility = 'unlisted',
                                            spoiler_text = None)
            
            return toot
        
        except MastodonAPIError as e:
            description = str(e).lower()
            try_count = try_count + 1

            # (Fragile) specific error catching. Would be better if Mastodon also return a simple token we can match.
            if '422' in description and 'Unprocessable Entity'.lower() in description and 'Try again in a moment'.lower() in description:
                if try_count >= 10:
                    raise Exception('Medias take too long to proceed')
                else:
                    llogger('medias are still processing, will retry in 10 seconds - ', e)
                    time.sleep(10)

            elif '422' in description and 'Unprocessable Entity'.lower() in description and 'Cannot attach a video to a post that already contains images'.lower() in description:
                if try_count >= 2:
                    raise Exception('Unexpected mixed images and videos')
                else:
                    llogger('mixed images and videos, will retry in 1 second with only videos - ', e)
                    medias_ids = videos_ids
                    time.sleep(1)

            elif '422' in description and 'Unprocessable Entity'.lower() in description and 'Cannot attach more than'.lower() in description:
                if medias_ids is None or len(medias_ids) == 0:
                    raise Exception('Unexpected too much attached medias')
                else:
                    llogger('too much medias, will retry in 1 second with last one removed - ', e)

                    medias_ids.pop()

                    if len(medias_ids) == 0:
                        medias_ids = None
                
                    time.sleep(1)

            elif '422' in description and 'Unprocessable Entity'.lower() in description and 'text character limit of'.lower() in description:
                llogger('toot is to big')
                return { 'id' : -2 } # This error will never solve, we don't want to retry forever: give an invalid toot_id to consider it as processed.

            elif '404' in description and 'the post you are trying to reply'.lower() in description:
                if try_count >= 2:
                    raise Exception('Unexpected "reply to" error')
                else:
                    llogger('the "reply to" id doesn\'t exist, will retry in 1 second without it - ', e)
                    in_reply_to_id = None
                    time.sleep(1)
            
            else:
                if try_count >= 5:
                    raise Exception('got an unknown API error - ' + str(e))
                else:
                    llogger('got an unknown API error, will retry in 10 seconds - ', e)
                    time.sleep(10)

        except Exception as e:
            raise

# Fetch Twitter tweet.
def fetch_tweet(tweet_url, tmp_dir_path = Path('/tmp/')):
    # Parse URL.
    parse_result = urlsplit(tweet_url)
    url_scheme = parse_result[0]
    url_host = parse_result[1]
    url_path = parse_result[2]
    clean_url = urlunsplit((url_scheme, url_host, url_path, None, None))

    # Parse path.
    path = PurePath(url_path)

    # Validate.
    if url_scheme != 'http' and url_scheme != 'https':
        raise Exception('invalid scheme "' + url_scheme + '"')

    if len(path.parts) != 4:
        raise Exception('invalid path "' + str(path) + '"')
    
    # Extract parts.
    twitter_username = path.parts[1]
    tweet_id = safe_int(path.parts[3])

    # Fetch tweet.
    tmp_sjson_path = tmp_dir_path.joinpath(twitter_username + '_' + str(tweet_id) + '.sjson')
    tmp_json_path = tmp_dir_path.joinpath(twitter_username + '_' + str(tweet_id) + '.json')

    for attempt in [ 0, 1, 2 ]:
        if attempt == 0:
            try:
                subprocess.run("twint -u '%s' -s 'since_id:%s and max_id:%s' --full-text --limit 1 --json -o '%s'" %
                                (twitter_username, str(tweet_id - 1), str(tweet_id), str(tmp_sjson_path)), shell = True, capture_output = True, check = True)

                subprocess.run("jq -s . '%s' > '%s'" % (str(tmp_sjson_path), str(tmp_json_path)), shell = True, capture_output = True, check = True)
                
                unlink_noerr(tmp_sjson_path)

                break
            except Exception as e:
                unlink_noerr(tmp_sjson_path)
                unlink_noerr(tmp_json_path)

        elif attempt == 1:
            try:
                subprocess.run("twint -u '%s' -s 'max_id:%s' --full-text --limit 20 --json -o '%s'" %
                                (twitter_username, str(tweet_id), str(tmp_sjson_path)), shell = True, capture_output = True, check = True)

                subprocess.run("jq -s . '%s' > '%s'" % (str(tmp_sjson_path), str(tmp_json_path)), shell = True, capture_output = True, check = True)
                
                unlink_noerr(tmp_sjson_path)

                break
            except Exception as e:
                unlink_noerr(tmp_sjson_path)
                unlink_noerr(tmp_json_path)
        
        else:
            unlink_noerr(tmp_sjson_path)
            unlink_noerr(tmp_json_path)
            
            return (twitter_username, tweet_id, None)

    # Parse JSON.
    try:
        tweets = json.load(open(tmp_json_path, 'r'))
    except Exception as e:
        return (twitter_username, tweet_id, None)
    finally:
        unlink_noerr(tmp_json_path)

    # Check we found the tweet
    for tweet in tweets:

        if safe_int(tweet['id']) != tweet_id and safe_int(tweet['conversation_id']) != tweet_id and tweet['link'].lower() != clean_url.lower():
            continue

        return (twitter_username, tweet_id, tweet)
    
    # Fallback.
    return (twitter_username, tweet_id, None)

# Download and recompress a video.
def download_video(video_url, video_path, max_video_size, logger = None):

    max_video_size_mib = max_video_size / (1024 * 1024)

    # Logger helper.
    def llogger(*args):
        if logger is not None:
            logger(*args)
    
    # Remove file, in case the directory is dirty.
    unlink_noerr(video_path)

    # Download video.
    llogger("downloading the video")

    try:
        subprocess.run("yt-dlp -o '%s' -N 8 -f b -S 'filesize~%sM' --recode-video mp4 --no-playlist --max-filesize 500M '%s'" %
                        (str(video_path), str(max_video_size_mib), video_url), shell = True, capture_output = False, check = True)
    except Exception as e:
        unlink_noerr(video_path)
        raise
    
    llogger("video downloaded")

    # Recompress
    try:
        tmp_video_path = video_path.with_name('tmp-' + video_path.name)
        pass_video_path_prefix = video_path.with_name(video_path.name + '-ffmpeg2pass')

        size = os.lstat(video_path).st_size
                    
        if size > max_video_size:
            llogger('video too big (%s > %s), recompressing' % (size, max_video_size))
            
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
            target_video_bitrate_kbit_s = (Decimal(max_video_size_mib) * Decimal(8192.0)) / (Decimal(1.048576) * duration_dec) - target_audio_bitrate_kbit_s
                        
            if target_video_bitrate_kbit_s <= Decimal(0):
                raise Exception('result in negative bitrate ', target_video_bitrate_kbit_s)

            # > Remove previous pass files, in case the directory is dirty.
            for path in pass_video_path_prefix.parent.glob(pass_video_path_prefix.name + '*'):
                unlink_noerr(path)

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
            llogger('unable to recompress video - ', e)

    finally:
        # > Remove pass files, in case the directory is dirty.
        for path in pass_video_path_prefix.parent.glob(pass_video_path_prefix.name + '*'):
            unlink_noerr(path)
        
        # > Remove temp file.
        unlink_noerr(tmp_video_path)



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

def log(*args):
    result = stringify(*args)

    if len(result) > 0:
        result = result[:1].upper() + result[1:]

    if len(result) > 0 and result[-1] != '.':
        result += '.'

    print(log_prefix, result.replace('\n', ' '))

# Create directory for Twitter account.
account_path = root_path.joinpath(twitter_account)

if account_path.exists():
    if not account_path.is_dir():
        log('cannot create directory "', account_path, '" because a file with this name altready exists')
        sys.exit(1)
else:
    try:
        os.mkdir(twitter_account)
    except Exception as e:
        log('cannot create directory "', account_path, '" - ', e)
        sys.exit(1)


# Open database.
sql_path = account_path.joinpath('tootbot.db')

try:
    # > "Connect"
    sql = sqlite3.connect(sql_path)
    db = sql.cursor()

    # > Update column names.
    columns = list(map(lambda x: x[1], db.execute('PRAGMA table_info(tweets)')))
    old_columns = { 'tweet', 'toot', 'twitter', 'mastodon', 'instance' }

    if len(columns) == 0:
        log('configure new database')

        db.execute('CREATE TABLE tweets (tweet_id INT, tweet_conversation_id INT, toot_id INT, twitter_account TEXT, mastodon_login TEXT, mastodon_instance TEXT)')
    
    if set(columns) == old_columns:
        log('update table columns names')

        db.execute('SAVEPOINT rename_tweets_table') # No need to `ROLLBACK TO SAVEPOINT``: changes before `RELEASE` will be lost when we exit on error anyway.
        db.execute('ALTER TABLE tweets RENAME COLUMN tweet TO tweet_id')
        db.execute('ALTER TABLE tweets RENAME COLUMN toot TO toot_id')
        db.execute('ALTER TABLE tweets RENAME COLUMN twitter TO twitter_account')
        db.execute('ALTER TABLE tweets RENAME COLUMN mastodon TO mastodon_login')
        db.execute('ALTER TABLE tweets RENAME COLUMN instance TO mastodon_instance')
        db.execute('ALTER TABLE tweets ADD COLUMN tweet_conversation_id INT')
        db.execute('RELEASE SAVEPOINT rename_tweets_table')
        sql.commit()

    # > Update column types.
    columns = { }
    
    for column in db.execute('PRAGMA table_info(tweets)'):
        columns[column[1]] = column[2].lower()
    
    if columns['tweet_id'] == 'text' or columns['toot_id'] == 'text' or columns['tweet_conversation_id'] == 'text':
        log('update table columns types')

        db.execute('SAVEPOINT retype_tweets_table')

        if columns['tweet_id'] == 'text':
            db.execute('ALTER TABLE tweets ADD COLUMN tweet_id_tmp INT')
            db.execute('UPDATE tweets SET tweet_id_tmp = tweet_id')
            db.execute('ALTER TABLE tweets DROP COLUMN tweet_id')
            db.execute('ALTER TABLE tweets RENAME COLUMN tweet_id_tmp TO tweet_id')
        
        if columns['toot_id'] == 'text':
            db.execute('ALTER TABLE tweets ADD COLUMN toot_id_tmp INT')
            db.execute('UPDATE tweets SET toot_id_tmp = toot_id')
            db.execute('ALTER TABLE tweets DROP COLUMN toot_id')
            db.execute('ALTER TABLE tweets RENAME COLUMN toot_id_tmp TO toot_id')

        if columns['tweet_conversation_id'] == 'text':
            db.execute('ALTER TABLE tweets ADD COLUMN tweet_conversation_id_tmp INT')
            db.execute('UPDATE tweets SET tweet_conversation_id_tmp = tweet_conversation_id')
            db.execute('ALTER TABLE tweets DROP COLUMN tweet_conversation_id')
            db.execute('ALTER TABLE tweets RENAME COLUMN tweet_conversation_id_tmp TO tweet_conversation_id')

        db.execute('RELEASE SAVEPOINT retype_tweets_table')
        sql.commit()

except Exception as e:
    log('cannot open database file "', sql_path, '" - ', e)
    sys.exit(1)

# Create application if it does not exist.
mastodon_secret_path = account_path.joinpath(mastodon_instance + '.secret')
mastodon_base_url = 'https://' + mastodon_instance

if not mastodon_secret_path.exists():
    if Mastodon.create_app(kAPP_NAME, api_base_url = mastodon_base_url, to_file = mastodon_secret_path):
        log('tootbot app created on instance "', mastodon_instance, '"')
    else:
        log('failed to create app on instance "', mastodon_instance, '"')
        sys.exit(1)


# Login to Mastodon.
log('login to Mastodon "' + mastodon_login + '"')

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
    log('login to Mastodon failed - ', e)
    sys.exit(1)


# Set locale to English, so we can more easily match error messages.
try:
    res = mastodon_api.set_language('en')
except Exception as e:
    log('failed to change Mastodon locale - ', e)

# Fecth Mastodon server configuration.
mastodon_supported_mime_type = [
    'abcd',
    'image/jpeg',
    'image/png',
    'image/gif',
    'image/heic',
    'image/heif',
    'image/webp',
    'image/avif',
    'video/webm',
    'video/mp4',
    'video/quicktime',
    'video/ogg',
    'audio/wave',
    'audio/wav',
    'audio/x-wav',
    'audio/x-pn-wave',
    'audio/vnd.wave',
    'audio/ogg',
    'audio/vorbis',
    'audio/mpeg',
    'audio/mp3',
    'audio/webm',
    'audio/flac',
    'audio/aac',
    'audio/m4a',
    'audio/x-m4a',
    'audio/mp4',
    'audio/3gpp',
    'video/x-ms-asf'
    ]
mastodon_image_size_limit = 10485760 # 10 MiB.
mastodon_video_size_limit = 41943040 # 40 MiB.
mastodon_max_characters = 500
mastodon_max_media_attachments = 4

try:
    mastodon_instance_result = mastodon_api.instance()

    mastodon_supported_mime_type = safe_dict(mastodon_instance_result, 'configuration.media_attachments.supported_mime_types', mastodon_supported_mime_type)
    mastodon_image_size_limit = safe_dict(mastodon_instance_result, 'configuration.media_attachments.image_size_limit', mastodon_image_size_limit)
    mastodon_video_size_limit = safe_dict(mastodon_instance_result, 'configuration.media_attachments.video_size_limit', mastodon_video_size_limit)
    mastodon_max_characters = safe_dict(mastodon_instance_result, 'configuration.statuses.max_characters', mastodon_max_characters)
    mastodon_max_media_attachments = safe_dict(mastodon_instance_result, 'configuration.statuses.max_media_attachments', mastodon_max_media_attachments)

except Exception as e:
    log('failed to fetch Mastodon server configuration, use default - ', e)


# Remove previous fetched tweets.
for path in account_path.glob('tweets.*json'):
    unlink_noerr(path)


# Fetch tweets.
log('fetching tweets')

twitter_sjson_path = account_path.joinpath('tweets.sjson')
twitter_json_path = account_path.joinpath('tweets.json')

try:
    subprocess.run("twint -u '%s' -tl --full-text --limit 10 --json -o '%s'" % (twitter_account, str(twitter_sjson_path)), shell = True, capture_output = True, check = True)
    subprocess.run("jq -s . '%s' > '%s'" % (str(twitter_sjson_path), str(twitter_json_path)), shell = True, capture_output = True, check = True)
except Exception as e:
    log('failed to fetch tweets - ', e)
    sys.exit(1)


# Load JSON.
try:
    tweets = json.load(open(twitter_json_path, 'r'))
except Exception as e:
    log('failed to parse tweets - ', e)
    sys.exit(1)

log('fetched ', len(tweets), ' tweets')

# Handle tweets.
for tweet in reversed(tweets):
    tweet_id = safe_int(tweet['id'])
    tweet_conversation_id = safe_int(tweet['conversation_id'])
    tweet_username = tweet['username']
    tweet_content_raw =  tweet['tweet']
    tweet_content = html.unescape(tweet_content_raw)

    toot_photos_ids = []
    toot_videos_ids = []


    # Check if this tweet has been processed.
    # > Check.
    try:
        db.execute('SELECT * FROM tweets WHERE tweet_id = ? AND twitter_account = ? and mastodon_login = ? and mastodon_instance = ? LIMIT 1', (tweet_id, twitter_account, mastodon_login, mastodon_instance))
        last = db.fetchone()

        if last:
            continue
    except Exception as e:
        log('cannot check if tweet ', tweet_id, ' exist in database - ', e)
        continue

    # > Set.
    def mark_tweet_as_processed(toot_id):
        db.execute("INSERT INTO tweets (tweet_id, tweet_conversation_id, toot_id, twitter_account, mastodon_login, mastodon_instance) VALUES (?, ?, ?, ?, ?, ?)", (tweet_id, tweet_conversation_id, toot_id, twitter_account, mastodon_login, mastodon_instance))
        sql.commit()


    # Log.
    log('--- ', str(tweet_id))
    log('content: "', tweet_content, '"')


    # We don't want to toot twitter replies (too much noise).
    if 'reply_to' in tweet and len(tweet['reply_to']) > 0:
        log('tweet skipped: it\'s a reply')
        mark_tweet_as_processed(-2)
        continue


    # Handle bogus RTs. They start with 'RT @username: '.
    bogus_rt_unrecoverable = re.match(r'^RT\s+@[^:]+:\s.*…', tweet_content, flags = re.DOTALL | re.IGNORECASE)
    bogus_rt_recoverable = re.match(r'^(RT\s+@([^:]+):\s).*', tweet_content, flags = re.DOTALL | re.IGNORECASE)

    if bogus_rt_unrecoverable is not None:
        log('tweet skipped: bogus reweet')
        mark_tweet_as_processed(-3)
        continue
    elif bogus_rt_recoverable is not None:
        # > Fix username, as a non-bogus RT.
        tweet_username = bogus_rt_recoverable.group(2)

        # > Remove the RT part in the content.`
        span = bogus_rt_recoverable.span(1)
        tweet_content = tweet_content[:span[0]] + tweet_content[span[1]:]
        
        # > Log.
        log('bogus retweet recovered: "', tweet_content, '"')


    # Check basic tweet content size.
    if len(tweet_content) > mastodon_max_characters:
        log('tweet skipped - too long ', len(tweet_content), ' > ', mastodon_max_characters)
        mark_tweet_as_processed(-1)
        continue


    # Handle retweet.
    if twitter_account and tweet_username.lower() != twitter_account.lower():
        new_tweet_content = ('🔄 @%s@twitter.com\n\n%s' % (tweet_username, tweet_content))

        if len(new_tweet_content) > mastodon_max_characters:
            log('retweet skipped - toot too long ', len(new_tweet_content), ' > ', mastodon_max_characters)
            mark_tweet_as_processed(-4)
            continue
        else:
            tweet_content = new_tweet_content            


    # Handle quoted tweet.
    # Note: a quoted tweet can be retweeted.
    quoted_tweet_images = None

    if 'quote_url' in tweet and tweet['quote_url'] != '':
        quote_url = tweet['quote_url']

        log('handle quoted tweet "', quote_url, '"')

        try:
            # > Fetch quoted tweet.
            fetch_result = fetch_tweet(quote_url, account_path)
            quoted_twitter_username = fetch_result[0]
            quoted_tweet = fetch_result[2]

            # > Generate quoted content.
            if quoted_tweet is None or 'tweet' not in quoted_tweet:
                log('failed to fetch quoted tweet "', quote_url, '"')
                quoted_content = ('@%s@twitter.com\n\n%s' % (quoted_twitter_username, quote_url))
            else:
                quoted_tweet_content_raw = quoted_tweet['tweet']
                quoted_tweet_content = html.unescape(quoted_tweet_content_raw)

                quoted_content = ('@%s@twitter.com\n\n%s' % (quoted_twitter_username, quoted_tweet_content))

                if 'photos' in quoted_tweet:
                    quoted_tweet_images = quoted_tweet['photos']

        except Exception as e:
            log('invalid quote url "', quote_url, '" - ', e)
            quoted_content = quote_url

        # > Generate tweet content.
        def create_quote_tweet(content):
            return tweet_content + ('\n\n———\n🔄 %s' % content)
        
        new_tweet_content = create_quote_tweet(quoted_content)

        if len(new_tweet_content) > mastodon_max_characters:
            log('toot too long with this quote, use reduced format')
            new_tweet_content = create_quote_tweet(quote_url)
        
        if len(new_tweet_content) > mastodon_max_characters:
            log('toot still too long with reduced format, skip')
            mark_tweet_as_processed(-5)
            continue

        tweet_content = new_tweet_content
        

    # Gather all links. Note: '\xa0' is unicode whitespace.
    links = re.findall(r'https?://[^\s\xa0]+', tweet_content)
    
    if 'photos' in tweet:
        links = links + tweet['photos']

    if quoted_tweet_images is not None:
        links = links + quoted_tweet_images


    # Handle links.
    handled_links = set()

    for link in links:

        # > Resolve link.
        dir_link = unredir(link)

        # > Check it wasn't already handled.
        if dir_link in handled_links:
            continue
        
        handled_links.add(dir_link)

        # > Check if we reached limits.
        if len(toot_videos_ids) > mastodon_max_media_attachments or len(toot_videos_ids) + len(toot_photos_ids) > mastodon_max_media_attachments:
            log('skip link "', link, '" -> "', dir_link, '" - limit of ', mastodon_max_media_attachments, ' medias reached')
            tweet_content = safe_replace(tweet_content, link, dir_link, mastodon_max_characters, log)
            continue

        # > Log.
        log('handle link "', link, '" -> "', dir_link, '"')

        # > Handle '/photo/' and '/video/' link as video.
        # > The gif animations are encoded as video, and stay under the '/photo/' path. If it's a real photo, it will just fail.
        is_photo_link = (re.search(r'twitter.com/.*/photo/', dir_link) is not None)
        is_video_link = (re.search(r'twitter.com/.*/video/', dir_link) is not None)

        if is_photo_link or is_video_link:
            video_path = account_path.joinpath('video.mp4')

            # > We consider that photos are in `tweet['photos']` with real link (different than this one), and so can be removed
            # >   from the the tweet content in all cases (succes or error).
            # > If we fail to upload the photo on next stage, the photo will be lost, but it's better than keeping the photo
            # >   *and* the link to it.
            if is_photo_link:
                tweet_content = tweet_content.replace(link, '')
                tweet_content = tweet_content.replace(dir_link, '')

            # Check that Mastodon server accept mp4 video.
            if 'video/mp4' not in mastodon_supported_mime_type:
                log('skip video "', dir_link, '": server doesn\'t support this type of video')
                continue

            # Download from Twitter, and upload to Mastodon.
            try:
                # > Download the video.
                log('download video "', dir_link, '"')

                download_video(dir_link, video_path, mastodon_video_size_limit, log)

                # > Read video content.
                file = open(video_path, "rb")
                video_data = file.read()
                file.close()

                # > Remove once not needed anymore.
                unlink_noerr(video_path)

                # > Check result size.
                if len(video_data) > mastodon_video_size_limit:
                    log('skip video - too big ', len(video_data), ' > ', mastodon_video_size_limit)
                    continue

                # > Post the video.
                log('upload video to Mastodon server')

                media_id = mastodon_media_post(mastodon_api, video_data, 'video/mp4', log)

                log('uploaded video - media-id: ', media_id)

                # > Store media id.
                toot_videos_ids.append(media_id)

                # > Remove the links to the video from the tweet content on success.
                if is_video_link:
                    tweet_content = tweet_content.replace(link, '')
                    tweet_content = tweet_content.replace(dir_link, '')
                
                # > Next link.
                continue

            except Exception as e:
                log('cannot upload video - ', e)
            
        # > Handle 'pbs.twimg.com'
        if 'https://pbs.twimg.com/' in dir_link:
            media = None

            # > Skip video thumbnails. Video are completely attached in previous section.
            if '/tweet_video_thumb/' in dir_link:
                log('skip thumbnail photo "', dir_link, '"')
                continue

            log('download photo "', dir_link, '"')

            # > Try by passing by nitter.
            if media is None:
                try:
                    media = requests.get(dir_link.replace('https://pbs.twimg.com/', 'https://nitter.net/pic/orig/'))
                except Exception as e:
                    log('failed to download the photo via nitter - ', e)

            # > Try by using the original link.
            if media is None:
                try:
                    media = requests.get(dir_link)
                except Exception as e:
                    log('failed to download the photo via original url - ', e)

            # > Post.
            if media is not None:
                try:
                    # > Check that Mastodon server accept this kind of photo.
                    content = media.content
                    content_type = media.headers.get('content-type')

                    if content_type.lower() not in mastodon_supported_mime_type:
                        log('skip photo "', dir_link, '": server doesn\'t support ', content_type, ' media')
                        continue

                    # > Check the size is okay.
                    if len(content) > mastodon_image_size_limit:
                        log('skip photo - too big ', len(content), ' > ', mastodon_image_size_limit)

                    # > Post the photo.
                    log('upload photo to Mastodon server')

                    media_id = mastodon_media_post(mastodon_api, content, content_type, log)

                    log('uploaded photo - media-id: ', media_id)

                    # > Store media id.
                    toot_photos_ids.append(media_id)

                    # > Remove the links to the photo from the tweet content on success.
                    tweet_content = tweet_content.replace(link, '')
                    tweet_content = tweet_content.replace(dir_link, '')

                    # > Next link.
                    continue

                except Exception as e:
                    log('cannot upload photo - ', e)

        # > Fallback: Handle other links.
        tweet_content = safe_replace(tweet_content, link, dir_link, mastodon_max_characters, log)


    # Remove ellipsis
    #tweet_content = tweet_content.replace('\xa0…', ' ')

    #c = c.replace('  ', '\n').replace('. ', '.\n')


    # Replace links to twitter by nitter ones.
    tweet_content = safe_replace(tweet_content, '/twitter.com/', '/nitter.net/', mastodon_max_characters, log)


    # Replace Twitter handles by Mastodon style handle:
    # - Avoid Twitter handles which may reference unrelated Mastodon users.
    # - Some Mastodon clients recognize these handles, and will create a link to Twitter.
    #
    # Note: we can't use re.sub: we need to skip entries which are already Mastodon
    #  handles (if someone Tweet a Mastodon handle for example), so we need to match
    #  a 'separator' caracter before and after Twitter handle, but doing so
    #  will make re.sub to don't see 2 handles separated by this separator (a space, for example).
    new_tweet_content = tweet_content

    while True:
        match = re.search(r'(^|[^a-zA-Z0-9_@])(@[a-zA-Z0-9_]{1,15})($|[^a-zA-Z0-9_@])', new_tweet_content)

        if match is None:
            break
    
        span1 = match.span(1) # First 'separator' group.
        span2 = match.span(3) # Second 'separator' group.

        #               ... (^|[...])]           + Twitter handle + @twitter.com   + [($|[...]) ...
        new_tweet_content = new_tweet_content[:span1[1]] + match.group(2) + '@twitter.com' + new_tweet_content[span2[0]:]
    
    if len(new_tweet_content) > mastodon_max_characters:
        log('replaced handles are too long, use original handles')
    else:
        tweet_content = new_tweet_content


    # Replace utm_? tracking.
    new_tweet_content = re.sub('\?utm.*$', '?utm_medium=Social&utm_source=Mastodon', tweet_content)

    if len(new_tweet_content) > mastodon_max_characters:
        log('replaced utm tracking is too long, use original utm')
    else:
        tweet_content = new_tweet_content


    # Add footer tags.
    if footer_tags:
        new_tweet_content = tweet_content + '\n' + footer_tags

        if len(new_tweet_content) > mastodon_max_characters:
            log('footer tags are too long, skip them')
        else:
            tweet_content = new_tweet_content

    
    # Check size.
    if len(tweet_content) > mastodon_max_characters:
        log('truncate toot, too long - ', len(tweet_content), ' > ', mastodon_max_characters)
        tweet_content = tweet_content[:mastodon_max_characters]


    # Check if this tweet is part of a conversation.
    toot_reply_to_id = None

    if tweet_conversation_id is not None:
        try:
            db.execute('SELECT toot_id FROM tweets WHERE tweet_conversation_id = ? AND twitter_account = ? and mastodon_login = ? and mastodon_instance = ? ORDER BY rowid DESC LIMIT 1', (tweet_conversation_id, twitter_account, mastodon_login, mastodon_instance))  # noqa
            last_tweet = db.fetchone()

            if last_tweet is not None:
                last_tweet_id = last_tweet[0]
            
                if last_tweet_id > 0:
                    toot_reply_to_id = last_tweet_id
       
        except Exception as e:
            log('cannot check if tweet ', tweet_id, ' is part of a conversation - ', e)


    # Post.
    if toot_reply_to_id is None:
        log('posting toot')
    else:
        log('posting toot as reply of toot ', toot_reply_to_id, ' - twitter conversation ',  tweet_conversation_id)

    try:
        # > Post the toot.
        toot = mastodon_post(mastodon_api, tweet_content, toot_reply_to_id, toot_photos_ids, toot_videos_ids, log)
        toot_id = safe_int(toot["id"])

        # > Mark as processed.
        mark_tweet_as_processed(toot_id)
        
        # > Log post.
        log('tweet ', tweet_id, ' created at ', tweet['created_at'], ' has been posted on ', mastodon_instance, ' - toot-id:', toot_id)

    except Exception as e:
        log('can\'t post toot - ', e)

# Done.
log('done')
print('')
