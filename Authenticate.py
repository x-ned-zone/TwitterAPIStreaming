import tweepy
import sys


# Authorize class
# This class uses the Tweepy package to communicate with twitter API to get and process data.
class Authenticate:
    def __init__(self):
        """"""
        return

    @classmethod
    def authenticate_app(cls, consumer_key, consumer_key_secret, access_token, access_token_secret):

        auth = tweepy.OAuthHandler(consumer_key, consumer_key_secret)
        auth.set_access_token(access_token, access_token_secret)
        api_connection = tweepy.API(auth)

        if not api_connection:
            print("Cannot authorize. Please check your keys!")
            sys.exit(-1)

        return api_connection
