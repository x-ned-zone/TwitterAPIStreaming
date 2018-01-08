import sys

import tweet_sms_pb2 as tweet_sms_pb
from SMS import *


class SerializeMessage:

    tweet_messages = tweet_sms_pb.Tweet_SMS()

    def __init__(self):
        """"""

    # private method __
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

            sms_pb.destinationServer = SMSobj.destinationServer
            sms_pb.destinationPort = SMSobj.destinationPort

        except Exception as a_error:
            print("Add SMS error: " + a_error.__str__())
            print("Passed values not validated")
            sys.exit()

    # private method
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

    def write_to_pb(self, filename_in, sms_data_in):
        """
        WRITE TO PROTOCOL BUFFER ...
        serializes the message and returns it as a string, with the bytes in string as a binary format.
        :param filename_in: protocol buffer filename to store serialized messages
        :param sms_data_in: structured data to serialize into protocol buffer
        :return: status
        """
        # ****** SERIALIZING TO PROTOCOL BUFFER ******

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

    def read_from_pb(self, filename_in):
        """
        READ FROM PROTOCOL BUFFER ... Read the existing tweet messages
        :param filename_in: name of file with with serialized messages
        :return: status
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
