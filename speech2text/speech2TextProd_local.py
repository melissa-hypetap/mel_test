#!/usr/bin/env python3
# -*- coding: utf-8 -c*-
"""
Created on Tue Aug  8 11:12:52 2023

@author: melissagray
"""

##import packages
import pymongo
import whisper
import ffmpeg
import pandas as pd
from pymongo import MongoClient
import boto3
import logging
import datetime
from datetime import datetime
from datetime import timezone
import numpy as np
import requests
import json
from time import sleep
import traceback 
import mongo_config
import transcribe_tools as tt



##set up python .log file
logging.basicConfig(filename = "speech2text.log",           #name of the log file
                    format='%(asctime)s - %(message)s',     #format the log to show timestamp then message
                    datefmt='%d-%b-%y %H:%M:%S',            #time object formatting
                    filemode = 'a',                         #append to previous logs
                    level=logging.INFO)                     #log level to the info level 


webhook = "https://hooks.slack.com/services/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"     #slack API webhook

#send a message to slack that the code has been initialised
payload_running = {"text" : "LOCAL✅speech2text initialised"}
webhook_result = tt.send_slack_message(payload_running , webhook)
logging.info('slack response: ' + f'{webhook_result.status_code} ' + f'{webhook_result.text}')      

# global time variables
ten_mins = 600
two_mins = 120
thirty_secs = 30
five_secs = 5


s3_client, s3_resource = tt.s3_connect_test()

model = tt.whisper_model_download()


while True:

    speechToTextQ = tt.connect_mongo_test()



    ## Get the collection locking record#this is the only collection locking record
    try:
        # Find the collection locking record in MongoDB
        collection_locking_record = tt.get_collection_locking_record() 
        if collection_locking_record is not None:      #if the document exists
            # Check if the status is LOCKED
            if collection_locking_record['status'][0] == 'LOCKED':
                # Check if the dateLastLocked is less than 10 minutes ago
                time_difference = tt.time_locked(collection_locking_record)   
                if time_difference.total_seconds() < ten_mins:       # if the collection locking record has been locked for less than 10 minutes, wait 5 seconds and look for it again 
                    #print("The collection is locked. Reading again...")
                    sleep(five_secs)
                else:   #if the collection locking record has been locked for over 10 minutes
                    #send a message to slack
                    payload = {"text": "❌The collection has been locked for 10 minutes or more."}
                    webhook_result = tt.send_slack_message(payload, webhook)
                    #log the error
                    logging.info('slack response: ' + f'{webhook_result.status_code} ' + f'{webhook_result.text}')   
                    logging.error("The collection has been locked for 10 minutes or more.")
                    raise Exception("The collection has been locked for 10 minutes or more.")
            elif collection_locking_record['status'][0] == 'UNLOCKED':       #if the collection locking recod is unlocked
                #log that it is unlocked and we are continuing
                logging.info('collection is unlocked - continue')                
                ##lock the collection locking record
                tt.lock_collection()
                #update the record in MongoDB
                df = tt.get_all_post_records()  
                if any(df):
                    ## check if any records are unlocked
                    if any(df['status'] == 'UNLOCKED'):     #if any unlocked entries exist
                        first_record = tt.lock_first_record(df)
                        ## unlock the collection
                        tt.unlock_collection()
                        ## filepath to save the video for transcription - the file will be overwritten once transcription is completed
                        filepath = '/Users/melissagray/Documents/Python/Whisper/temporary_download/video1.mp4'   
                        ##run the transcription
                        try:
                            # download the video file from AWS S3 bucket
                            # x= record["storageId"]  #get the AWS storageId
                            # logging.info("storageId of file is" + x)
                            # bucket_name = record["storageBucket"]   #get the AWS bucket name
                            # s3_client.download_file(bucket_name, x, filepath) #download the video from the specified S3 bucket
                            #run whisper
                            #audio = whisper.load_audio(filepath)
                            result = tt.transcribe_video(first_record, s3_client, filepath, model)
                            
                            #write record to speechToTextResult collection
                            tt.write_transcription_record(first_record, result)
                            
                            #add the storage id of the video that was just proccessed to the log along with a completed message
                            logging.info(f'{first_record["storageId"]=}')
                            logging.info("transcription complete")
                        except Exception as e:  #if an error occurs during the dowloading/ transcription phase
                            print(e) 
                            result2 = {'text' : None} 
                            
                            tt.write_transcription_record(first_record, result2)
                            
                            #log that there is an error - if the vide has no sound then no transcription will be possible
                            logging.info(f'{first_record["storageId"]=}')
                            logging.info("transcription failed check for sound")
                            logging.info(f'{e}')
                            pass        #allow the code to continue running if an error is found in the file download / transcription part of the code as whisper will throw an error if the video has no audio
    
                        while True:
                            # Find the collection locking record document in MongoDB
                            collection_locking_record = tt.get_collection_locking_record() 
                            if collection_locking_record is not None:      #if the collection locking record exists
                                if collection_locking_record['status'][0] == 'LOCKED':
                                    time_difference = tt.time_locked(collection_locking_record)   #find how long ago the collection was locked
                                    if time_difference.total_seconds() < ten_mins:       #if the collection was locked less than 10 minutes ago
                                        #wait 5 seconds and then retry 
                                        sleep(five_secs)
                                    else:       #if the collection was locked less than 10 minutes ago
                                        #send a message to slack
                                        payload = {"text": "❌The collection is still locked after 10 minutes - could not write result for" + first_record['_id']}
                                        webhook_result = tt.send_slack_message(payload, webhook)
                                        #log that the collection has been locked for over 10 minutes
                                        logging.info('slack response: ' + f'{webhook_result.status_code} ' + f'{webhook_result.text}')    
                                        logging.error("The collection is still locked after 10 minutes - could not write result for" )
                                        logging.info(f'{first_record["storageId"]=}')  
                                        #stop the code
                                        raise Exception("The collection is still locked after 10 minutes - could not write result for" + first_record['_id'])
                                elif collection_locking_record['status'][0] == 'UNLOCKED':       #else, if the collection is unlocked
                                    #print to console
                                    print('collection is unlocked - continue')
                                    ## lock the collection collection
                                    tt.lock_collection()
                                    ##delete the record that we have just transcribed from speechToTextQ
                                    tt.delete_record(first_record)
                                    ##unlock the speechToTextQ collection
                                    tt.unlock_collection()
                                    print('complete')   #print to console
                                    break   #exit the while loop
                            else:   #if no unlocked entries exist or all entries have been locked for over 10 minutes
                                print('no unlocked entries')         
                                raise Exception('no unlocked entries')
                
                            break  # Exit the loop when the collection is unlocked
                    else:       #else no unlocked entries exist
                    #get the current time in UTC
                        time_difference_record = tt.time_since_record_locked(df)   #calculate how long since the most recently locked record was locked
                        if time_difference_record.total_seconds() < ten_mins:        #if the most recently locked record has been locked for less than 10 minutes
                            #wait 5 seconds and check again to see if any records have been unlocked
                            sleep(five_secs)
                        else:   #else, all remaining records have been locked for over 10 minutes
                            #send a message to slack
                            payload = {"text": "❌One or more records has been locked for over 10 minutes."}
                            webhook_result = tt.send_slack_message(payload, webhook)
                            #log the error
                            logging.info('slack response: ' + f'{webhook_result.status_code} ' + f'{webhook_result.text}')   
                            logging.error("One or more records have been locked for over 10 minutes.")
                            #unlock the collection
                            tt.unlock_collection()
                            print("No matching record found - unlocking colection.")    #print to console
                            # log that all records have been proccessed 
                            logging.info('speech2text complete on all available records')
                            sleep(two_mins)  #wait 2 minutes before checking for new records
                else:   #else, all remaining records have been locked for over 10 minutes
                    #send a message to slack
                    payload = {"text": "❌No records available."}
                    webhook_result = tt.send_slack_message(payload, webhook)
                    #log the error
                    logging.info('slack response: ' + f'{webhook_result.status_code} ' + f'{webhook_result.text}')   
                    logging.error("No records available")
                    #unlock the collection
                    tt.unlock_collection()
                    print("No records availabe - unlocking colection.")    #print to console
                    # log that all records have been proccessed 
                    logging.info('no records available to process')
                    sleep(two_mins)  #wait 2 minutes before checking
    except Exception as e:
     # Handle exceptions and send a message to Slack
     error_message = f" ‼️ An error occurred: {str(e)}\n\n"
     error_message += traceback.format_exc()  # Get the traceback for detailed error info
     payload_error = {"text": error_message}
     webhook_result = tt.send_slack_message(payload_error, webhook)
     #add errors to log
     logging.info('slack response: ' + f'{webhook_result.status_code} ' + f'{webhook_result.text}')   
     logging.error(error_message)
     
     #unlock collection
     tt.unlock_collection()
     # Wait for a short time before attempting to restart
     sleep(thirty_secs)  # Adjust the delay as needed         