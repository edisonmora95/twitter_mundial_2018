import tweepy
import json
import sys
import os
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv(verbose=True, dotenv_path='./.env')

consumer_key = os.getenv("CONSUMER_KEY")
consumer_secret = os.getenv('CONSUMER_SECRET')
access_token = os.getenv('ACCESS_TOKEN')
access_secret = os.getenv('ACCESS_SECRET')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')


class MyStreamListener(tweepy.StreamListener):

    def on_connect(self):
        print('Connected to Streaming API!')

    def on_data(self, raw_data):
        print('Tweet collected')
        data_json = json.loads(raw_data)
        db.tweets.insert_one(data_json)

    def on_status(self, status):
        print(status.text)

    def on_error(self, status_code):
        client.close()
        return False

    def on_timeout(self):
        print('Timeout', file=sys.stderr)
        client.close()
        return True


if __name__ == '__main__':
    # Initialize DB
    url = f"mongodb://{DB_USER:}:{DB_PASSWORD:}@ds159110.mlab.com:59110/twitter_mundial_2018"
    client = MongoClient(url)

    db = client.twitter_mundial_2018

    # Initialize stream
    listener = MyStreamListener()

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)

    stream = tweepy.Stream(auth, listener)

    stream.filter(locations=[-80.9677654691, -4.95912851321, -75.2337227037, 1.3809237736])
