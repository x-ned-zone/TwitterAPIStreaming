import json
from Message import *

#        Tweet
#          |
#       Message
#          |
#         / \
#       SMS  MMS
#       /     \
#   (Text)   (Text + Multi-media)


class SMS(Message):
    """ Map tweets to comparable SMS (with consistent mapping of twitter handles to phone numbers) """
    def __init__(self, t_id, date, tweet_text, user_id, user_name, screen_name, source, user_location,
                 text_language, message_id):
        super(SMS, self).__init__(t_id, date, tweet_text, user_id, user_name, screen_name, source, user_location,
                                  text_language, message_id)

    """API call to authenticate this app"""
    def authenticate(self):
        """ This is to allow the app to access and use an API for an SMS gateway"""

    """encode message object into json"""
    def encode_in_json(self):

        tweet_data = {"tweet_id": self.tweet_id, "created_at": self.date, "media_source": self.media_source,
                      "text": self.message, "text_lang": self.text_language, "user": {} }

        tweet_data["user"]["id"] = self.user_id
        tweet_data["user"]["name"] = self.user_name
        tweet_data["user"]["screen_name"] = self.user_screen_name
        tweet_data["user"]["location"] = self.user_location

        self.message_json = json.dumps(tweet_data)
        return self.message_json

    """get string representation of message"""

    def get_message(self):
        message = 'TweetID: %s, \nDate: %s, \nMessage: %s, \nuserID: %s, \nuserName: %s, \nuserScreenName: %s,' \
                  '\n[ MediaSource: %s, userlocation: %s, Text_language: %s]' % (self.tweet_id, self.date, self.message,
                                                                                 self.user_id, self.user_name,
                                                                                 self.user_screen_name, self.media_source,
                                                                                 self.user_location, self.text_language)
        message = "SMS Message: \n" + \
                  message \
                  + ("\nDestination Server: %s" % self.destinationServer +
                     "\nDestination Port: %s" % self.destinationPort)
        return message
