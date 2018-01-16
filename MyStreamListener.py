import tweepy
import json
import time
import sys
from langdetect import detect
from langdetect.lang_detect_exception import LangDetectException

from SerializeMessage import SerializeMessage
from TwitterAPIStreaming import TwitterAPIStreaming


class MyStreamListener(tweepy.StreamListener):
    """ This is a stream listener class, which streams data from the twitter API and stores it in a data structure"""

    def __init__(self, tweet_messages_pb, duration, start_time, option, full_spritzer, t_buffer, me_index):
        self.out_serialize_file = tweet_messages_pb
        self.streaming_duration = duration
        self.start_time = start_time
        self.serialize_option = option
        self.full_spritzer_ = full_spritzer
        self.t_buffer = t_buffer
        self.me_index = me_index

    """Override methods from Tweepy Stream ***
    *** [START] """
    def on_data(self, raw_data):
        """Override method to get the raw data processed"""

        if type(raw_data) != str:
            print("\nOn_data error: raw_data not a string")
            exit()

        start = time.time()
        tweet_json = json.loads(raw_data)

        try:
            # filter out delete status.
            # Extract only tweet that is not a retweet, to avoid repetition of the same tweets.

            if time.time()-self.start_time <= self.streaming_duration or self.full_spritzer_:
                if raw_data and ("delete" not in tweet_json):
                        # and (not tweet_json["retweeted"] or 'RT @' not in tweet_json["text"]):

                    tweet_text = tweet_json['text'].strip()
                    if "extended_tweet" in tweet_json:
                        tweet_text = tweet_json['extended_tweet']['full_text'].strip()

                    text_language = "N/A"
                    try:
                        text_language = detect(tweet_text)
                    except LangDetectException:
                        pass

                    # tweet_json["user"]["lang"]

                    end = time.time()
                    duration = end - TwitterAPIStreaming.s_time

                    if len(self.t_buffer) == 1:
                        TwitterAPIStreaming.s_time = start
                    if duration < 1.00:
                        TwitterAPIStreaming.m_count += 1
                    elif TwitterAPIStreaming.s_time > 0 and duration >= 1.00:
                        # print("Received-tweets : ", TwitterAPIStreaming.m_count)
                        # print("Time-frame : %f second(s)" % duration)
                        # print()
                        TwitterAPIStreaming.s_time = start
                        TwitterAPIStreaming.m_count = 0

                    if self.me_index[0] % 2 == 0:
                        sys.stdout.write("Streaming ... Tweet [ %i ] \r" % (self.me_index[0] + 1))
                    else:
                        sys.stdout.write("Streaming ... Tweet [ %i ] \r" % (self.me_index[0] + 1))
                    sys.stdout.flush()

                    serialize = SerializeMessage(self.out_serialize_file,
                                                 self.serialize_option,
                                                 tweet_json['id'],
                                                 tweet_json['created_at'], tweet_text,
                                                 tweet_json['user']['id'], tweet_json['user']['name'],
                                                 tweet_json['user']['screen_name'], tweet_json['source'],
                                                 tweet_json['user']['location'], text_language, self.t_buffer,
                                                 self.me_index)
                    serialize.start()
                    serialize.join()

                    return True
            else:
                return False

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
        return False

    def on_limit(self, track):
        # time.sleep(seconds=60*15)
        sys.stderr.write(track)
        print("\nAPI query limitation reached!")

    def on_delete(self, status_id, user_id):
        print("delete status, with id= %s", status_id)

    def on_connect(self):
        print("\nConnection established")
        return

    def on_disconnect(self, notice):
        print("\nConnection disconnected. \nNotice: ", notice)
        # time.sleep(15*60)
        pass

    def re_connect(self):
        print("\nReconnecting ...")
        return
    """ [END] """

