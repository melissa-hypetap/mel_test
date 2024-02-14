#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 25 10:44:36 2023

@author: melissagray
"""

import ffmpeg
import cv2
import boto3
import pymongo
from pymongo import MongoClient
import pandas as pd
import uuid
import frame_extract_tools as ft
import mongo_config
from time import sleep
import logging
import traceback
import requests
import json


# set up python .log file
logging.basicConfig(filename="frame_extract.log",  # name of the log file
                    # format the log to show timestamp then message
                    format='%(asctime)s - %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S',  # time object formatting
                    filemode='a',  # append to previous logs
                    level=logging.INFO)  # log level to the info level


# slack API webhook
webhook = "https://hooks.slack.com/services/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"

# send a message to slack that the code has been initialised
payload_running = {"text": "dev✅frame_extract initialised"}
webhook_result = ft.send_slack_message(payload_running, webhook)
logging.info('slack response: ' +
             f'{webhook_result.status_code} ' + f'{webhook_result.text}')

# global variables
ten_mins = 600
two_mins = 120
thirty_secs = 30
five_secs = 5
last_error_message = None

#test connection to s3
s3_client, s3_resource = ft.s3_connect_test()



while True:
    try:
        # Find the collection locking record in MongoDB
        collection_locking_record = ft.get_collection_locking_record()
        if collection_locking_record is not None:  # if the document exists
            # Check if the status is LOCKED
            if collection_locking_record['status'][0] == 'LOCKED':
                # Check if the dateLastLocked is less than 10 minutes ago
                time_difference = ft.time_locked(collection_locking_record)
                if time_difference.total_seconds() < ten_mins:
                    # if the collection locking record has been locked for less than 10 minutes, wait 5 seconds and look for it again
                    sleep(five_secs)
                else:
                    payload = {"text": "❌The collection has been locked for 10 minutes or more" }
                    webhook_result = ft.send_slack_message(payload, webhook)
                    # log that the collection has been locked for over 10 minutes
                    logging.info('slack response: ' + f'{webhook_result.status_code} ' + f'{webhook_result.text}')
                    logging.error("The collection is still locked after 10 minutes")
                    # Raise an exception
                    raise Exception("The collection has been locked for 10 minutes or more.")
                    
            # if the collection locking recod is unlocked
            elif collection_locking_record['status'][0] == 'UNLOCKED':
                #lock the collection
                ft.lock_collection()                                                                                                                                        
                # get all the current records
                df = ft.get_all_post_records()
                if any(df):
                    # check if any records are unlocked
                    if any(df['status'] == 'UNLOCKED'):  # if any unlocked entries exist
                    #lock the first avaialble UNLOCKED record
                        first_record = ft.lock_first_record(df)
                        # unlock the collection
                        ft.unlock_collection()                                                                                                                              
                        # filepath to save the video for transcription - the file will be overwritten once transcription is completed
                        filepath = '/Users/melissagray/Documents/Python/ffmpeg_tests/temporary_download/video1.mp4'
                        # run the transcription
                        while True:
                            try:
                                #run frame_capture function
                                result_df = ft.frame_capture(first_record, s3_client, filepath)

                                # write record to videoFrameLabelDetectionQ collection
                                ft.write_all_frames(result_df)
                                # add the storage id of the video that was just proccessed to the log along with a completed message
                                logging.info(f'{first_record["storageId"]=}')
                                logging.info("frame extract complete")
                                
                                #check the collection locking record again
                                collection_locking_record = ft.get_collection_locking_record()
                                if collection_locking_record is not None:   # if the collection locking record exists
                                    if collection_locking_record['status'][0] == 'LOCKED': #if it is locked
                                        # find how long ago the collection was locked
                                        time_difference = ft.time_locked(collection_locking_record)
                                        if time_difference.total_seconds() < ten_mins: 
                                            # if the collection was locked less than 10 minutes ago
                                            # wait 5 seconds and then retry
                                            sleep(five_secs)
                                        else:  # if the collection was locked more than 10 minutes ago
                                            # send a message to slack
                                            payload = {"text": "❌The collection is still locked after 10 minutes - could not remove record with _id =" + first_record['_id']}
                                            webhook_result = ft.send_slack_message(payload, webhook)
                                            # log that the collection has been locked for over 10 minutes
                                            logging.info('slack response: ' + f'{webhook_result.status_code} ' + f'{webhook_result.text}')
                                            logging.error("The collection is still locked after 10 minutes - could not remove record with _id =")
                                            logging.info(f'{first_record["_id"]=}')
                                            
                                            # raise an Exception
                                            raise Exception("The collection is still locked after 10 minutes - could not remove record with _id =" + first_record['_id'])
                                    
                                    elif collection_locking_record['status'][0] == 'UNLOCKED':   # else, if the collection is unlocked
                                        # print to console
                                        print('collection is unlocked - continue')
                                        # lock the collection collection
                                        ft.lock_collection()                                                                                                                                           
                                        # delete the record that we have just transcribed from speechToTextQ
                                        ft.delete_record(first_record)
                                        # unlock the videoLabelDetectionQ collection
                                        ft.unlock_collection()                                                                                                                                          
                                        print('complete')  # print to console
                                        break  # exit the while loop
                            except Exception as e:  # if an error occurs during the dowloading/ frame splitting phase
                                print(e)
                                # log that there is an error -
                                logging.info(f'{first_record["storageId"]=}')
                                logging.info("frame extraction for record failed")
                                logging.info(f'{e}')
                                break # exit the while loop
                    else:  # else no unlocked entries exist
                        # calculate how long since the most recently locked record was locked
                        ft.unlock_collection()
                                                                                                                                               
                        time_difference_record = ft.time_locked(df)
                        # if the most recently locked record has been locked for less than 10 minutes
                        if time_difference_record.total_seconds() < ten_mins:
                            # wait 5 seconds and check again to see if any records have been unlocked
                            sleep(five_secs)
                        else:  # else, all remaining records have been locked for over 10 minutes
                            # send a message to slack
                            payload = {"text": "❌One or more records has been locked for over 10 minutes."}
                            webhook_result = ft.send_slack_message(payload, webhook)
                            # log the error
                            logging.info('slack response: ' + f'{webhook_result.status_code} ' + f'{webhook_result.text}')
                            logging.error("One or more records have been locked for over 10 minutes.")
                            # unlock the collection
                            ft.unlock_collection()                                                                                                                                                  
                            # print to console
                            print("No matching record found - unlocking colection.")
                            # log that all records have been proccessed
                            logging.info('speech2text complete on all available records')
                            # wait 2 minutes before checking for new records
                            sleep(two_mins)
                else:  # else,  no more records
                    # send a message to slack
                    payload = {"text": "❌No records available."}
                    webhook_result = ft.send_slack_message(payload, webhook)
                    # log the error
                    logging.info('slack response: ' + f'{webhook_result.status_code} ' + f'{webhook_result.text}')
                    logging.error("No records available")
                    # unlock the collection
                    ft.unlock_collection()                                                                                                                                                          
                    # print to console
                    print("No records availabe - unlocking colection.")
                    # log that all records have been proccessed
                    logging.info('no records available to process')
                    sleep(two_mins)  # wait 2 minutes before checking
                    raise Exception("❌No records available.")
    except Exception as e:
        # Handle exceptions and send a message to Slack
        error_message = f" ‼️ An error occurred: {str(e)}\n\n"
        # Get the traceback for detailed error info
        error_message += traceback.format_exc()
        if error_message != last_error_message:   #check if the error message is new - if new send to slack
            payload_error = {"text": error_message}
            webhook_result = ft.send_slack_message(payload_error, webhook)
            # add errors to log
            logging.info('slack response: ' + f'{webhook_result.status_code} ' + f'{webhook_result.text}')
            logging.error(error_message)
            last_error_message = error_message

        # Wait for a short time before attempting to restart
        sleep(thirty_secs)  # Adjust the delay as needed
        
