from Authenticate import Authenticate
from MyStreamListener import *
from SMS import SMS

from socket import *
import os
import sys
from http.client import IncompleteRead   # Python 3
from urllib3.exceptions import ProtocolError


class TwitterAPIStreaming:

    tweets_buffer = []
    m_index = [0]
    s_time = 0.0
    m_count = 0
    # average tweets per second
    average_tweets = 0

    def __init__(self, api_handle, output_file, duration, serialize_option, full_spritzer):

        self.apiHandle = api_handle

        self.streaming_duration = duration
        self.output_file = output_file
        self.duration = duration
        self.serialize_option = serialize_option
        self.full_spritzer = full_spritzer

    def stream_tweets(self):
        
        start_time = time.time()

        print()

        if self.full_spritzer:
            print("ST: Streaming full twitter spritzer (sample)")
        else:
            print("ST: Streaming duration : %s seconds" % self.streaming_duration)
        print("ST: Streaming Tweets in progress ... ")

        continue_stream = True
        while continue_stream:
            try:
                # tweets_public
                # [1] Create Listener. [2] Initialize data
                stream_listener = MyStreamListener(tweet_messages_pb=self.output_file, duration=self.streaming_duration,
                                                   start_time=start_time, option=self.serialize_option,
                                                   full_spritzer=self.full_spritzer,
                                                   t_buffer=TwitterAPIStreaming.tweets_buffer,
                                                   me_index=TwitterAPIStreaming.m_index)

                # [3] Start the stream with the specified listener class
                my_stream = tweepy.Stream(auth=self.apiHandle.auth, listener=stream_listener, retry_time=10)

                # [4] Tell the stream listener to retrieve (filter) only the twitter spritzer sample
                MyStreamListener(my_stream.sample(), duration=self.streaming_duration, start_time=start_time,
                                 option=self.serialize_option, full_spritzer=self.full_spritzer,
                                 t_buffer=TwitterAPIStreaming.tweets_buffer,
                                 me_index=TwitterAPIStreaming.m_index)

                return "done"

            except tweepy.TweepError as te_error:
                my_stream.disconnect()
                print("Stream tweets error : \n", te_error.__str__())

            except ProtocolError as pe:

                TRACE(pe.__str__() + "\nProtocolError: \n" + "time : ")

                # reconnect to api stream
                my_stream.running = True
                print("\nReconnecting ...")
                continue_stream = True
                continue

            except IncompleteRead as ir:
                print("\nIncompleteRead error : \n", ir.__str__())
                continue

            except KeyboardInterrupt:
                # allow keyboard exiting ctr+c
                my_stream.disconnect()
                continue_stream = False

            except ConnectionError as conn_err:
                print("\nConnection error : ", conn_err.__str__())
                pass

            except Exception as other_error:
                print("\nOther error :", other_error)
                pass

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
            sms_list = SerializeMessage(binary_file).read_from_pb(binary_file)

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


def TRACE(log_text):
    from datetime import datetime
    t_time = time.time()
    print("\n%s : %f\n" % (log_text, t_time))
    try:
        f_log = open("TRACE.txt", "a+")
        f_log.write("\n%s : %f" % (log_text, time.time()))
        f_log.write("\nDatetime : %s" % datetime.fromtimestamp(t_time).strftime('%c'))
        f_log.close()
    except Exception as err:
        print(err.__str__())
        print("TRACE.txt not found. New file created")


def main():
    """ Define application key, key-secret and access-token """
    consumer_key = "g104Mx068t8JqSOfCCGPJ5xhx"
    consumer_key_secret = "suuzYhQCAQQJAT2K4LqAOVWeB9ZtKPp01jFA8KDPUGTdoYgc5l"
    access_token = "937943519985094657-W0MJHdYgUoJ4eHqleO4HpUedYjjprZI"
    access_token_secret = "ZGiEnXPCJbrNFmgBImKyUG55ImNXanVllpwQ0Nnt1K1Jp"

    # if len(sys.argv) != n: print "usage: python 'TwitterAPIStreaming.py 'pb filename''"
    # file = open(sys.argv[1], "rb")
    filename = "media/twitter_spritzer"
    full_spritzer = False
    option = 0
    streaming_duration = float(60*10)   # Enter 0 for a full spritzer

    print()
    from SerializeMessage import SerializeMessage
    try:
        """ ================ USER INPUT  ================ """
        print("Please enter (blank for default test values)... ")
        dest_server = input(" * File Destination Server-address : ")
        dest_port = input(" * File Destination Port-number : ")

        try:
            option = \
                input("\nChoose a serialising option (enter a number): \n 1. Protocol buffer \n 2. Json \n\nOption : ")
            option = int(option)
        except Exception:
            print("Error : Option should be a number: 1 or 2")

        if streaming_duration == float(0):
            full_spritzer = True

        if dest_server == "":
            dest_server = "localhost"
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
            os.remove("TRACE.txt")
        except OSError:
            print("\n[Remove]: '%s' not found" % filename)
            pass

        """ Authenticate application with the application keys """
        api_handle = Authenticate().authenticate_app(consumer_key, consumer_key_secret,
                                                     access_token, access_token_secret)

        query_stream = TwitterAPIStreaming(api_handle, output_file=filename, duration=float(streaming_duration),
                                           serialize_option=option, full_spritzer=full_spritzer)

        TRACE("Start Stream time")
        query_stream.stream_tweets()
        TRACE("End Stream time")

        # Serialize the json buffer with tweets to json file
        if option == 2:
            ser_json = SerializeMessage(filename)
            ser_json.export_json(TwitterAPIStreaming.tweets_buffer)

        print("\n============================================")
        print("API Streaming complete")
        print("============================================")
        print("Tweet Counter = %i tweet(s)" % TwitterAPIStreaming.m_index[0])
        print("Tweets file size = %f MB" % (float(os.stat(filename).st_size)/1000000.0))

        print("\nData serialized to : ", filename)
        print("File destination : [ Server = %s, Port = %s ]" % (dest_server, dest_port))

        # """ Deserialize messages """
        # query_stream.deserialize_messages(binary_file=filename, ser_option=option)

        """ SMS transfer """
        # query_stream.send_serialized_file(filename, d_server=dest_server, d_port=dest_port, option=option)

    except Exception as exp:
        print("error : ", exp.__str__())


if __name__ == "__main__":
    main()
