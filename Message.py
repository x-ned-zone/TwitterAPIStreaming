from abc import ABC, abstractmethod

#       Message
#          |
#         / \
#       SMS  MMS
#       /     \
#   (Text)   (Text + Multi-media) -> include 'multi_media' = {"image": {}, "audio": {}, "video": {}}


class Message (ABC):
    """abstract class Message"""
    tweet_id = 0
    message_id = 0
    message = ""
    date = ""
    media_source = ""
    text_language = ""

    user_id = 0
    user_name = ""
    user_screen_name = ""
    user_location = ""

    # default values
    destinationServer = ""
    destinationPort = 0

    multi_media = {"image": {}, "audio": {}, "video": {}}

    """ Map tweets to comparable SMS (with consistent mapping of twitter handles to phone numbers) """
    def __init__(self, t_id, date, tweet_text, user_id, user_name, screen_name, source, user_location, text_language,
                 message_id):

        self.message_id = message_id
        self.tweet_id = t_id
        self.message = tweet_text
        self.date = date

        # [ user id, name, screen_name, location, media source]
        self.user_id = user_id
        self.user_name = user_name
        self.user_screen_name = screen_name
        self.user_location = user_location
        self.media_source = source

        self.text_language = text_language

        self.destinationServer = "localhost"  # "127.0.0.1
        self.destinationPort = 80

    def set_multi_media(self, m_media_list):
        return

    @abstractmethod
    def get_message(self):
        """
        override this method to customize message display
        :return:
        """
        return
