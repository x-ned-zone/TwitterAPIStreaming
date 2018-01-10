import os
import tweepy
import json
import time
import sys
from langdetect import detect
from SerializeMessage import SerializeMessage
from langdetect.lang_detect_exception import LangDetectException

from http.client import IncompleteRead # Python 3
from urllib3.exceptions import ProtocolError

from SMS import SMS


class MyStreamListener(tweepy.StreamListener):
    """ This is a stream listener class, which streams data from the twitter API and stores it in a data structure"""

    def __init__(self, tweet_messages_pb, filter_lang, duration, start_time, option, full_spritzer):
        self.out_serialize_file = tweet_messages_pb
        self.filter_languages = filter_lang
        self.streaming_duration = duration
        self.start_time = start_time
        self.serialize_option = option
        self.full_spritzer_ = full_spritzer

    """Override methods from Tweepy Stream
    ************************** [START] ************************** """

    def on_status(self, status):
        try:
            self.on_data(status)
        except Exception as e:
            print("on_status Error : ", e.__str__())

    def on_data(self, raw_data):
        """Override method to get the raw data processed"""

        if type(raw_data) != str:
            print("\nOn_data error: raw_data not a string")
            exit()

        tweet_json = json.loads(raw_data)

        try:
            # filter out delete status.
            # Extract status of selected language.
            # Extract only tweet that is not a retweet, to avoid repetition of the same tweets.

            if time.time()-self.start_time <= self.streaming_duration or self.full_spritzer_:
                if raw_data and ("delete" not in tweet_json) \
                        and (tweet_json["user"]["lang"] == "en") \
                        and (not tweet_json["retweeted"] and 'RT @' not in tweet_json["text"]):

                    tweet_text = tweet_json['text'].strip()
                    if "extended_tweet" in tweet_json:
                        tweet_text = tweet_json['extended_tweet']['full_text'].strip()

                    tweet_id = tweet_json['id']
                    date = tweet_json['created_at']
                    source = tweet_json['source']
                    user_id = tweet_json['user']['id']
                    user_name = tweet_json['user']['name']
                    user_screen_name = tweet_json['user']['screen_name']
                    user_location = tweet_json['user']['location']

                    text_language = "N/A"
                    try:
                        text_language = detect(tweet_text)
                    except LangDetectException as lang_ex:
                        pass

                    if not text_language == "N/A" and \
                            (text_language in self.filter_languages or self.filter_languages == "all"):
                        if self.serialize_option == 1:
                            self.export_protob(self.out_serialize_file, tweet_id, date, tweet_text, user_id, user_name,
                                               user_screen_name, source, user_location, text_language)
                        elif self.serialize_option == 2:
                            self.export_json(self.out_serialize_file, tweet_id, date, tweet_text, user_id, user_name,
                                             user_screen_name, source, user_location, text_language)
                    else:
                        pass

                    return True
            else:
                return False

        except ProtocolError:
            print ("ProtocolError ... pass")
            pass

        except IncompleteRead:
            print ("IncompleteRead ... pass")
            pass

        except UnicodeDecodeError as d:
            print("decode error: " + d.message)

        except UnicodeEncodeError as e:
            print("encode error: " + e.message)

        except Exception as e:
            print("Exception error: " + e.__str__())

    # return False in on_data disconnects the stream
    def on_error(self, status_code):
        if status_code == 420:
            return False

    def on_timeout(self):
        print("time out!")

    def on_limit(self, track):
        time.sleep(seconds=60*15)

        sys.stderr.write(track)
        print("API query limitation reached!")

    def on_delete(self, status_id, user_id):
        print("delete status, with id= %s", status_id)

    """ [END] """

    """ CLASS METHODS """
    """ ************************** [START] ************************** """
    def truncate_text(self, text_length, text, N):
        """Truncate/split text to multi parts texts of length n"""
        text_collection = []
        for s_index in range(0, text_length, N):
            atext = text[s_index: s_index + N]
            text_collection.append(atext)
        return text_collection

    def export_json(self, messages_file, tweet_id, date, tweet_text, user_id, user_name, user_screen_name,
                    source, user_location, text_language):

        # Generate a multipart SMS for tweets longer than 140 characters.
        multi_text = self.truncate_text(len(tweet_text), tweet_text, N=140)

        # Create SMS object(s) from the message splits
        # Write to message to json file
        m_index = 0
        for text_i in multi_text:
            m_index += 1

            tweet_json = {"tweet_id": tweet_id, "date": date, "tweet_text": text_i, "user_id": user_id,
                          "user_name": user_name, "user_screen_name": user_screen_name, "user_location": user_location,
                          "text_lan": text_language, "source": source, "message_id": m_index}

            # SMS >> JSON FILE
            try:
                with open(messages_file, mode="a+") as jsonFile:
                    json.dump(tweet_json, jsonFile)
                    jsonFile.write("\n")
                    jsonFile.truncate()

            except Exception as w_error:
                print("w_error: " + w_error.__str__())

    def export_protob(self, messages_file, tweet_id, date, tweet_text, user_id, user_name, user_screen_name,
                      source, user_location, text_language):

        serialize = SerializeMessage()

        # Generate a multipart SMS for tweets longer than 140 characters.
        multi_text = self.truncate_text(len(tweet_text), tweet_text, N=140)

        # Create SMS object(s) from the message splits
        # Write to message to protocol buffer
        m_index = 0
        for text in multi_text:
            m_index += 1
            anSMS = SMS(tweet_id, date, text, user_id, user_name, user_screen_name, source, user_location, text_language,
                        message_id=m_index)

            # SMS >> TO >> PROTOCOL BUFFER
            serialize.write_to_pb(messages_file, anSMS)
