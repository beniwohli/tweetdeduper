# -*- coding: utf-8 -*-
import datetime
import json
import os

import logging
from opbeat.base import Client
from opbeat.handlers.logging import OpbeatHandler

logging.basicConfig(level=logging.INFO)

opbeat_client = Client(app_id=os.environ['OPBEAT_APP_ID'])

logger = logging.getLogger(__name__)
logger.addHandler(OpbeatHandler(opbeat_client))

import pymongo
import requests
from requests_oauthlib import OAuth1

client = pymongo.MongoClient(os.environ.get('MONGOLAB_URI', None))
db = client.get_default_database()

auth = OAuth1(
    os.environ['CONSUMER_KEY'],
    client_secret=os.environ['CONSUMER_SECRET'],
    resource_owner_key=os.environ['OAUTH_TOKEN'],
    resource_owner_secret=os.environ['OAUTH_TOKEN_SECRET'],
)
following = os.environ['FOLLOWING']


def screen_name_to_id(screen_name):
    response = requests.post(
        'https://api.twitter.com/1.1/users/lookup.json',
        data={'screen_name': screen_name},
        auth=auth,
    )
    data = json.loads(response.content.decode('utf-8'))
    if len(data):
        return data[0]['id']


def short_urls(tweet):
    return [
        url['display_url']
        for url in tweet['entities']['urls']
        if url['display_url'].startswith('theverge.com/e/')
    ]


def is_dupe(tweet):
    urls = short_urls(tweet)
    dupe = False
    for url in urls:
        result = db.vd.tweets.find_one({'url': url})
        if not result:
            result = {'url': url, 'tweet_ids': []}
        if tweet['id_str'] not in result['tweet_ids']:
            result['tweet_ids'].append(tweet['id_str'])
        if min(map(int, result['tweet_ids'])) == int(tweet['id_str']):
            result['text'] = tweet['text']
        result['dupes'] = len(result['tweet_ids']) - 1
        result['last_update'] = datetime.datetime.now()
        db.vd.tweets.update(
            {'url': url},
            result,
            upsert=True,
        )
        dupe = dupe or bool(result['dupes'])
    return dupe


def backfill(max_tweets=1000):
    total = 0
    max_id = None
    params = {
        'q': 'from:' + following,
        'count': 100,
    }
    while total < max_tweets:
        data = requests.get(
            'https://api.twitter.com/1.1/search/tweets.json',
            params=params,
            auth=auth,
        )
        data = json.loads(data.content.decode('utf-8'))
        for item in data['statuses']:
            max_id = int(item['id_str'])
            is_dupe(item)
        total += len(data['statuses'])
        if not len(data['statuses']):
            break
        params['max_id'] = max_id - 1


def retweet(tweet):
    if os.environ['TWEETIT'] == 'yes':
        response = requests.post(
            'https://api.twitter.com/1.1/statuses/retweet/%s.json' % tweet['id_str'],
            auth=auth,
        )


def listen():
    stream = requests.post(
        'https://stream.twitter.com/1.1/statuses/filter.json',
        data={'follow': screen_name_to_id(following)},
        auth=auth,
        stream=True,
    )

    for line in stream.iter_lines():
        if line:
            try:
                data = json.loads(line.decode('utf-8'))
                if data['user']['screen_name'] == following:
                    if not is_dupe(data):
                        logger.info(
                            'Retweeting %s',
                            data['text'],
                            extra={'tweet': data}
                        )
                        retweet(data)
                    else:
                        logger.info(
                            'Not retweeting duped %s', data['text'],
                            extra={'tweet': data}
                        )
            except Exception:
                logger.error(
                    'Error processing %s',
                    line.decode('utf-8'),
                    exc_info=True,
                )


if __name__ == '__main__':
    try:
        db.vd.tweets.ensure_index('url', unique=True)
        db.vd.tweets.ensure_index('last_update', expireAfterSeconds=72*3600)
        backfill()
        listen()
    except Exception:
        opbeat_client.captureException(exc_info=True)
        raise
