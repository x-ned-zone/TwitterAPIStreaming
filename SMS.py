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

    """get string representation of message"""
    def get_message(self):
        message = 'TweetID: %s, \nDate: %s, \nMessage: %s, \nuserID: %s, \nuserName: %s, \nuserScreenName: %s,' \
                  '\n[ MediaSource: %s, userlocation: %s, Text_language: %s]' % (self.tweet_id, self.date, self.message,
                                                                                 self.user_id, self.user_name,
                                                                                 self.user_screen_name, self.media_source,
                                                                                 self.user_location, self.text_language)
        message = "SMS Message: \n" + message
        return message
