from Authenticate import Authenticate
from MyStreamListener import *
from socket import *
import os
import sys
from http.client import IncompleteRead # Python 3
from urllib3.exceptions import ProtocolError

class TwitterAPIStreaming:
    streaming_duration = 0.0

    def __init__(self, apiHandle):
        self.apiHandle = apiHandle

    # Map from tweets in file to SMS objects

    def stream_tweets(self, output_file, duration, filter_language, serialize_option, full_spritzer):
        
        start_time = time.time()
        self.streaming_duration = duration

        print()
        if (full_spritzer):
            print("ST: Streaming full twitter spritzer (sample)")
        else:
            print("ST: Streaming duration : %s seconds" % self.streaming_duration)
        print("ST: Streaming Tweets in progress ... ")

        continue_st = True
        while continue_st :
            try:
                # tweets_public
                # [1] Create Listener. [2] Initialize data
                stream_listener = MyStreamListener(tweet_messages_pb=output_file, filter_lang=filter_language,
                                                   duration=self.streaming_duration, start_time=start_time,
                                                   option=serialize_option, full_spritzer=full_spritzer)

                # [3] Start the stream with the specified listener class
                my_stream = tweepy.Stream(auth=self.apiHandle.auth, listener=stream_listener)

                # [4] Tell the stream listener to retrieve (filter) only the twitter spritzer sample
                MyStreamListener(my_stream.sample(), filter_lang=filter_language,
                                 duration=self.streaming_duration, start_time=start_time, 
                                 option=serialize_option, full_spritzer=full_spritzer)

                return "done"
            except ProtocolError:
                print ("TAPIS: ProtocolError ... pass")
                continue

            except IncompleteRead:
                print ("TAPIS: IncompleteRead ... pass")
                continue

            except tweepy.TweepError as te_error:
                my_stream.disconnect()
                print("Get_tweets error: ", te_error.__str__())
            continue_st = False
            # break

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

            # Protocol buffer 
            if serialize_option == 1:
                pb_file = open(pb_serialized_file, "rb")
                file_data = pb_file.read()
                pb_file.close()

            # Json
            elif serialize_option == 2:
                json_file = open(pb_serialized_file, "r")
                file_data = json_file.read()
                json_file.close()

            # send data to server
            client_socket.send(file_data)
            client_socket.close()
            return "file sent!"

        except Exception as s_error:
            print("Error send: " + s_error.__str__())
            return "file not sent!"

    def reformat_json(self, filename):
        """Correct json data format. Insert all items in a list"""
        if os.stat(filename).st_size == 0:
            print("file not found")

        else:
            with open(filename, "r+") as rw_file:
                """"""
                json_list = []
                for item in rw_file.read().split("\n"):

                    if len(item) > 0:
                        json_list.append(json.loads(item))

                rw_file.seek(0)
                json.dump(json_list, rw_file, indent=4)
                rw_file.truncate()

    @staticmethod
    def deserialize_messages(binary_file, ser_option):
        """
        This method retrieves the tweets data from the protocol buffer

        :param binary_file:
        :param ser_option
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
    """ Define application key, key-secret and access-token """
    consumer_key = "g104Mx068t8JqSOfCCGPJ5xhx"
    consumer_key_secret = "suuzYhQCAQQJAT2K4LqAOVWeB9ZtKPp01jFA8KDPUGTdoYgc5l"
    access_token = "937943519985094657-W0MJHdYgUoJ4eHqleO4HpUedYjjprZI"
    access_token_secret = "ZGiEnXPCJbrNFmgBImKyUG55ImNXanVllpwQ0Nnt1K1Jp"

    # if len(sys.argv) != n: print "usage: python 'TwitterAPIStreaming.py 'pb filename''"
    # file = open(sys.argv[1], "rb")
    filename = "media/twitter_spritzer"
    filter_languages = ["en"]
    full_spritzer_i = False
    option = 0
    streaming_duration = float(0) # Enter 0 for a full spritzer

    """ ================ USER INPUT  ================ """
    print()
    print("Please enter following inputs (blank for default test values)... ")
    dest_server = input (" * File Destination Server-address : ") 
    dest_port = input (" * File Destination Port-number : ") 
    lang = input (" * Tweets Language Code [en, af,] seperated by comma (leave blank for 'en-english') : ") 

    try:
        option = \
            input("\nChoose a serialising option (enter a number): \n 1. Protocol buffer \n 2. Json \n\nOption : ")
        option = int(option)
    except Exception:
        print("Error : Option should be a number: 1 or 2")

    for lang_i in lang.split(","):
        filter_languages.append(lang_i)

    if streaming_duration == float(0):
        full_spritzer_i = True
   
    if dest_server == "":
        dest_server = "localhost"
    if dest_port == 0 or dest_port=="":
        dest_port = 80

    if option == 1:
        filename += ".bin" 
        print("... Data Serialising set to Protocol buffer.")
    elif option == 2:
        filename += ".json"
        print("... Data Serialising set to Json.")
    else:
        print("Option not found. Please enter option 1 or 2.")
        exit()

    # Start clean. Delete the file if it already exists
    try:
        os.remove(filename)
        # print("\n... File already exists. Deleted!")
    except OSError:
        print("\n[Remove]: File not found")
        pass

    """ Authenticate application with the application keys """
    apiHandle = Authenticate().authenticate_app(consumer_key, consumer_key_secret, access_token, access_token_secret)
    query_stream = TwitterAPIStreaming(apiHandle)

    """ Get tweets from Streaming API and Map tweets/SMSes to protocol buffers """
    query_stream.stream_tweets(output_file=filename, duration=float(streaming_duration), filter_language=filter_languages,
                                   serialize_option=option, full_spritzer=full_spritzer_i)
    
    print("\nAPI Streaming complete :)")

    if option == 2:
        query_stream.reformat_json(filename)

    print("\nData serialized to : ", filename)
    print("File destination : [ Server = %s, Port = %s ]" % (dest_server, dest_port))

    # """ Deserialize messages """
    # query_stream.deserialize_messages(binary_file=filename, ser_option=option)

    """ SMS transfer """
    # query_stream.send_serialized_file(filename, d_server=dest_server, d_port=dest_port, option=option)


if __name__ == "__main__":
    main()
