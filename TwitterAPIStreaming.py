from Authenticate import Authenticate
from MyStreamListener import *
from socket import *
import os
import sys


class TwitterAPIStreaming:
    streaming_duration = 0.0

    def __init__(self, apiHandle):
        self.apiHandle = apiHandle

    # Map from tweets in file to SMS objects

    def stream_tweets(self, output_file, duration, filter_language, serialize_option):
        start_time = time.time()
        self.streaming_duration = duration

        print("\nST: Streaming Tweets")
        print("streaming duration : %s seconds" % self.streaming_duration)

        try:
            # tweets_public
            # [1] Create Listener. [2] Initialize data
            stream_listener = MyStreamListener(tweet_messages_pb=output_file, filter_lang=filter_language,
                                               duration=self.streaming_duration, start_time=start_time,
                                               option=serialize_option)

            # [3] Start the stream with the specified listener class
            my_stream = tweepy.Stream(auth=self.apiHandle.auth, listener=stream_listener)

            # [4] Tell the stream listener to retrieve (filter) only the twitter spritzer sample
            MyStreamListener(my_stream.sample(), filter_lang=filter_language,
                             duration=self.streaming_duration, start_time=start_time, option=serialize_option)
            return "done"

        except tweepy.TweepError as te_error:
            my_stream.disconnect()
            print("Get_tweets error: ", te_error.__str__())

    """send message object"""
    @staticmethod
    def send_serialized_file(pb_serialized_file, d_server, d_port, serialize_option):

        try:
            # send 'this'
            # Stream the results, encoded in JSON to a TCP socket

            # Create a TCP Client (socket)
            client_socket = socket(AF_INET, SOCK_STREAM)

            # connect to server
            client_socket.connect((d_server, d_port))

            # send data to server
            # Protocol buffer
            if serialize_option == "1":
                pb_file = open(pb_serialized_file, "rb")
                f_data = pb_file.read()
                pb_file.close()

            # Json
            elif serialize_option == 2:
                pb_file = open(pb_serialized_file, "r")
                f_data = pb_file.read()
                pb_file.close()

            client_socket.send(f_data)
            client_socket.close()
            return "success"

        except Exception as s_error:
            print("Error send: " + s_error.__str__())
            return "failed"

    def reformat_Json(self, filename):
        # if os.stat(filename).st_size == 0:
        #     print("file not found")

        with open(filename, "r+") as rw_file:
            """"""
            json_list = []
            for item in rw_file.read().split("\n"):
                if len(item) > 0:
                    json_list.append(json.loads(item))

            # print("List -> ", json_list.__dict__)

            rw_file.seek(0)
            json.dump(json_list, rw_file)
            rw_file.truncate()

    @staticmethod
    def deserialize_messages(binary_file, ser_option):
        """
        This method retrieves the tweets data from the protocol buffer

        :param binary_file:
        :return: sms_list
        """

        print("\nDeserialize_messages\n====================")

        if os.stat(binary_file).st_size == 0:
            print("file has no content")

        # protocol buffer
        if ser_option == 1:
            sms_list = SerializeMessage().read_from_pb(binary_file)

        # json
        elif ser_option == 2:
            sms_list = []
            with open(binary_file, "r") as in_file:
                json_obj = json.load(in_file)

                try:
                    for anSMS in json_obj:
                        print(anSMS)
                        SMSobj = SMS(anSMS["tweet_id"], anSMS["date"], anSMS["tweet_text"], anSMS["user_id"],
                                     anSMS["user_name"], anSMS["user_screen_name"], anSMS["source"],
                                     anSMS["user_location"], anSMS["text_lan"], anSMS["message_id"])

                        sms_list.append(SMSobj)
                        print(SMSobj.get_message(), end="\n\n")
                except Exception as l_error:
                    print("List SMS error (json) : " + l_error.__str__())
                    sys.exit()

        # read status
        status = "failed"
        if len(sms_list) > 0:
            status = "success"

        print("*** Read status = %s ***" % status)

        return sms_list


def main():
    """ ================ INPUT VARIABLES ================ """
    """ Define application key, key-secret and access-token """
    consumer_key = "g104Mx068t8JqSOfCCGPJ5xhx"
    consumer_key_secret = "suuzYhQCAQQJAT2K4LqAOVWeB9ZtKPp01jFA8KDPUGTdoYgc5l"
    access_token = "937943519985094657-W0MJHdYgUoJ4eHqleO4HpUedYjjprZI"
    access_token_secret = "ZGiEnXPCJbrNFmgBImKyUG55ImNXanVllpwQ0Nnt1K1Jp"

    """Get this input from user"""
    # command args or input
    # if len(sys.argv) != n: print "usage: python 'TwitterAPIStreaming.py 'pb filename''"
    # file = open(sys.argv[1], "rb")

    filename = "media/twitter_spritzer"
    streaming_duration = float(10 * 1)  # stream tweets for n minutes
    filter_languages = ["en"]
    dest_server = "41.193.221.138"
    dest_port = 80

    try:
        option = \
            input("\nChoose a serialising option (enter a number): \n 1. protocol buffer \n 2. json \n\nOption:")
        option = int(option)

    except Exception:
        print("Error : Please enter a number")

    if option == 1:
        filename += ".bin"
        print("... Serialising set to Protocol buffer")
    elif option == 2:
        filename += ".json"
        print("... Serialising set to Json")
    else:
        print("Option not found. Please enter option 1 or 2")
        exit()

    print("\nFilename : ", filename)
    print("File destination : [ Server= %s, Port= %s ]" % (dest_server, dest_port))

    # delete the file if it exists
    try:
        os.remove(filename)
        print("\n... File already exists. Deleted!")
    except OSError:
        pass

    if len(filename) == 0:
        print("ST error: Tweets output file missing!")
        exit()
    if streaming_duration < 10:
        print("ST error: Duration must at least be >= 10")
        exit()

    """ Authenticate application with the application keys """
    apiHandle = Authenticate().authenticate_app(consumer_key, consumer_key_secret, access_token, access_token_secret)
    query_stream = TwitterAPIStreaming(apiHandle)

    # query_stream.flushFile(filename)

    """ Get tweets from Streaming API and Map tweets/SMSes to protocol buffers """
    query_stream.stream_tweets(output_file=filename, duration=streaming_duration, filter_language=filter_languages,
                               serialize_option=option)

    if option == 2:
        query_stream.reformat_Json(filename)

    # """ Deserialize messages """
    query_stream.deserialize_messages(binary_file=filename, ser_option=option)

    # SMS storage & config

    """ SMS transfer """
    # query_stream.send_serialized_file(filename, d_server=dest_server, d_port=dest_port, option=option)


if __name__ == "__main__":
    main()
