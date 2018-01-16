from TwitterAPIStreaming import *
from SMS import SMS

import sys
import tweet_sms_pb2 as tweet_sms_pb
import json
import threading


class SerializeMessage(threading.Thread):

    """ CLASS METHODS """
    """ ************************** [START] ************************** """
    def __init__(self, *args):

        if len(args) == 1:
            self.messages_file = args[0]
        else:
            threading.Thread.__init__(self)
            self.messages_file = args[0]
            self.option_s = args[1]
            self.tweet_id = args[2]
            self.date = args[3]
            self.tweet_text = args[4]
            self.user_id = args[5]
            self.user_name = args[6]
            self.user_screen_name = args[7]
            self.source = args[8]
            self.user_location = args[9]
            self.text_language = args[10]
            self.t_buffer = args[11]
            self.me_index = args[12]

    def run(self):
        # Protocol buffer
        if self.option_s == 1:
            self.export_protob()
        else:
            self.buffer_json()

    def truncate_text(self, text_length, text, N):
        """Truncate/split text to multi parts texts of length n"""
        text_collection = []
        for s_index in range(0, text_length, N):
            atext = text[s_index: s_index + N]
            text_collection.append(atext)
        return text_collection

    def buffer_json(self):

        # Generate a multipart SMS for tweets longer than 140 characters.
        # Create multiple SMS object(s) from the message splits and,
        for text_i in self.truncate_text(len(self.tweet_text), self.tweet_text, N=140):

            self.me_index[0] += 1

            self.t_buffer.append(
                {"tweet_id": self.tweet_id,
                 "date": self.date,
                 "tweet_text": text_i,
                 "user_id": self.user_id,
                 "user_name": self.user_name,
                 "user_screen_name": self.user_screen_name,
                 "user_location": self.user_location,
                 "text_lan": self.text_language,
                 "source": self.source,
                 "message_id": self.me_index[0]}
            )

    def export_json(self, t_buffer):
        # Write Tweet(SMS) >> JSON FILE
        try:
            with open(self.messages_file, mode="a+") as jsonFile:
                json.dump(t_buffer, jsonFile, indent=4)
                jsonFile.truncate()
        except Exception as w_error:
            print("export_json error: " + w_error.__str__())

    def export_protob(self):
        # Generate a multipart SMS for tweets longer than 140 characters.

        # Create SMS object(s) from the message splits
        # Write to message to protocol buffer

        for text in self.truncate_text(len(self.tweet_text), self.tweet_text, N=140):

            self.me_index[0] += 1

            anSMS = SMS(self.tweet_id, self.date, text, self.user_id, self.user_name, self.user_screen_name,
                        self.source, self.user_location, self.text_language, message_id=self.me_index[0])

            # Tweet-SMS >> TO >> PROTOCOL BUFFER
            self.write_to_pb(self.messages_file, anSMS)

    # =================================================================================================================
    tweet_messages = tweet_sms_pb.Tweet_SMS()

    # private function - buffer access/read/write
    def __add_message(self, sms_pb, SMSobj):
        """
        :param sms_pb:
        :param SMSobj:
        :return:
        """

        try:
            if SMSobj.user_location is None or SMSobj.user_location is "None":
                user_location = "No location"
            else:
                user_location = SMSobj.user_location

            sms_pb.tweet_id = SMSobj.tweet_id
            sms_pb.message_id = SMSobj.message_id
            sms_pb.message_text = SMSobj.message
            sms_pb.date = SMSobj.date
            sms_pb.media_source = SMSobj.media_source
            sms_pb.text_language = SMSobj.text_language

            sms_pb.user_id = SMSobj.user_id
            sms_pb.user_name = SMSobj.user_name
            sms_pb.user_screen_name = SMSobj.user_screen_name
            sms_pb.user_location = user_location

        except Exception as a_error:
            print("Add SMS error: " + a_error.__str__())
            print("Passed values not validated")
            sys.exit()

    # private function - buffer access/read
    def __list_message(self, messages_pb):
        """
        :param messages_pb:
        :return:
        """
        sms_list = []
        try:
            for anSMS in messages_pb.smses:
                SMSobj = SMS(anSMS.tweet_id, anSMS.date, anSMS.message_text, anSMS.user_id,
                             anSMS.user_name, anSMS.user_screen_name, anSMS.media_source, anSMS.user_location,
                             anSMS.text_language, anSMS.message_id)

                print(SMSobj.get_message(), end="\n\n")

                sms_list.append(SMSobj)

        except Exception as l_error:
            print("List SMS error: " + l_error.__str__())
            sys.exit()

        return sms_list

    # =================================================================================================================
    # setter function - direct disk read/write
    def write_to_pb(self, filename_in, sms_data_in):
        """
        WRITE / SERIALIZING TO PROTOCOL BUFFER ...
        serializes the message and returns it as a string, with the bytes in string as a binary format.
        :param filename_in: protocol buffer filename to store serialized messages
        :param sms_data_in: structured data to serialize into protocol buffer
        :return: status
        """
        try:
            # [1] READ EXISTING BUFFER
            try:
                pb_file = open(filename_in, "rb")
                self.tweet_messages.ParseFromString(pb_file.read())
                pb_file.close()

            except IOError:
                print("file '"+filename_in+"' not found. Creating new file.")

            # [2] ADD MESSAGE TO EXISTING BUFFER
            # Add a message to protocol buffer
            self.__add_message(self.tweet_messages.smses.add(), SMSobj=sms_data_in)

            # [3] WRITE MESSAGE BACK TO DISK
            # Write the new message back to the disk
            pb_file = open(filename_in, "wb")
            pb_file.write(self.tweet_messages.SerializeToString())
            pb_file.close()
            w_status = "success"

        except Exception as wb_error:
            print("wb_error: " + wb_error.__str__())
            w_status = "failed"

        return w_status

    # getter function - direct disk read
    def read_from_pb(self, filename_in):
        """
        READ FROM PROTOCOL BUFFER ... Read the existing tweet messages
        :param filename_in: name of file with with serialized messages
        :return: list
        """
        print("\n****** READING FROM PROTOCOL BUFFER ****** \n=================================================")
        try:
            pb_file = open(filename_in, "rb")
            self.tweet_messages.ParseFromString(pb_file.read())
            pb_file.close()

            return self.__list_message(self.tweet_messages)

        except Exception as rb_error:
            print("rb_error: " + rb_error.__str__())

        return None
