#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct  4 15:45:49 2023

@author: melissagray
"""
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

client = MongoClient("localhost", 27017)
db = client['hypetap']
speechToTextQ = db["speechToTextQ"]
webhook = "https://hooks.slack.com/services/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"  


def lock_collection():
    collectionlocking = { "recordType" : "COLLECTION_LOCKING_RECORD" }
    lock = { "$set": { "status": "LOCKED", "dateLastLocked" : datetime.now(timezone.utc), 'lockedBy' : 'transcriber1'} }        #set status to LOCKED, dateLastLocked to current time in UTC, and lockedBy to be transcriber1   
    speechToTextQ.update_one(collectionlocking, lock)  
    
def unlock_collection():
    collectionlocking = { "recordType" : "COLLECTION_LOCKING_RECORD" }  #get the collection locking record
    newvalues = { "$set": { "status": "UNLOCKED", "dateLastUnLocked" : datetime.now(timezone.utc), 'lockedBy' : None} }   #  #set status to UNLOCKED, dateLastUnLocked to current time in UTC, and lockedBy to be transcriber1   
    speechToTextQ.update_one(collectionlocking, newvalues)     
    
def get_collection_locking_record():
    collection_record = {"recordType" : "COLLECTION_LOCKING_RECORD"} #this is the only collection locking record
    result= speechToTextQ.find(collection_record)
    collection_record_df = pd.DataFrame(list(result))
    return collection_record_df


def time_locked(collection_record_df):
    date_last_locked = collection_record_df['dateLastLocked'][0].tz_localize('utc')
    current_time = datetime.now(timezone.utc)
    time_difference = current_time - date_last_locked   
    return(time_difference)

def get_all_post_records():
    get_all = {"recordType" : "UPDATE_POST_RECORD"} #query entire collection **SHOULD THIS BE recordType : UPDATE_POST_RECORD ?
    ## extract query results to a python dataframe
    cursor = speechToTextQ.find(get_all)
    df = pd.DataFrame(list(cursor)) 
    return df

def lock_first_record(df):
    first_record = df[df.status == 'UNLOCKED'].iloc[0]  
    get_first_record = { "_id": first_record["_id"] } # The _id of the first unlocked regord                 
    ## lock the record record
    newvalues = { "$set": { "status": "LOCKED", "dateLastLocked" : datetime.now(timezone.utc), 'lockedBy' : 'transcriber1'} }     #set status to LOCKED, dateLastLocked to current time in UTC, and lockedBy to be transcriber1 
    speechToTextQ.update_one(get_first_record, newvalues)
    return first_record

def delete_record(record):
    current_record = {'_id' : record['_id']}    #get the _id of the record we have just processed
    speechToTextQ.delete_one(current_record) 




def transcribe_video(record, s3_client, filepath, model):
    x= record["storageId"]  #get the AWS storageId
    logging.info("storageId of file is" + x)
    bucket_name = record["storageBucket"]   #get the AWS bucket name
    s3_client.download_file(bucket_name, x, filepath) #download the video from the specified S3 bucket
    #run whisper
    audio = whisper.load_audio(filepath)
    result = model.transcribe(audio)
    return result 



def write_transcription_record(first_record, result):
    speechToTextResult = db["speechToTextResult"]  
    first_record = first_record.replace({np.nan : None})
    #get the speechToTextResult collection from MongoDB
    record_create = pd.DataFrame(first_record).replace(np.nan, None).transpose()  #create the record with all the information from the speechToTextQ record
    record_edit_fields= record_create.drop(['bucketName', 'storageBucket', 'status', 'dateLastLocked', 'dateLastUnLocked', 'lockedBy', 'recordType', '_class'], axis=1, errors='ignore')    #remove the unwanted fields 
    record_dict = record_edit_fields.to_dict('records') 
    #convert to a dictionary object
    speechToTextResult.insert_one(record_dict[0])
    query2 = {'_id' : first_record['_id']}    #get the _id of the record we just processed
    newvalues = { "$set": { "transcript": result['text'] } }    #add the new transcribe field with the whisper result
    speechToTextResult.update_one(query2, newvalues)
    

def time_since_record_locked(df):    
    date_last_locked_record = min(df[df['status'] == 'LOCKED']['dateLastLocked']).tz_localize('utc')        #find the most recently locked record
    current_time2 = datetime.now(timezone.utc)                  #get the current time in UTC
    time_difference_record = current_time2 - date_last_locked_record  
    return time_difference_record


def send_slack_message(payload, webhook):           #define a function to send messages to slack
    """Send a Slack message to a channel via a webhook. 
    
    Args:
        payload (dict): Dictionary containing Slack message, i.e. {"text": "This is a test"}
        webhook (str): Full Slack webhook URL for your chosen channel. 
    
    Returns:
        HTTP response code, i.e. <Response [503]>
    """

    return requests.post(webhook, json.dumps(payload))

                 #update the record in MongoDB





def connect_mongo_test():
    while True:
        try:
            client = MongoClient("localhost", 27017)
            db = client['hypetap']
            videoToFramesQ = db["speechToTextQ"]
            client.server_info() # force connection on a request to verify connection
             ## DS - Do not send this webhook - instead we will rely on the exception webook since this one is being sent with every mongo connection
            return videoToFramesQ
        except pymongo.errors.ConnectionFailure as err:
            # if connection to the MongoDB database fails
                #send a message to slack
            payload = {"text": "LOCAL TEST🛑Connection to MongoDB refused. See error message:\n\n\n"  + str(err) + 'trying again in 10 minutes'}
            webhook_result = send_slack_message(payload, webhook)
            # add the error to the log
            logging.info('slack response: ' + f'{webhook_result.status_code} ' + f'{webhook_result.text}')   
            logging.error("Connection to MongoDB refused")
            # exit the code
            #raise Exception('Connection to MongoDB refused')
            sleep(600)

        
def whisper_model_download():
    try:
        #load the model
        model = whisper.load_model("small")
        #message to slack that the model is loaded
        payload_running = {"text" : "LOCAL TEST✅whisper model loaded"}
        webhook_result = send_slack_message(payload_running , webhook)
        #add to log
        logging.info('slack response: ' + f'{webhook_result.status_code} ' + f'{webhook_result.text}')
        logging.info('whisper model loaded successfully')
        return model
    except Exception as e:
        #if there is an error printi it in the console
        print(e)
        #message to slack that there is an error
        payload_running = {"text" : "LOCAL TEST🛑 whisper model failed to load"}
        webhook_result = send_slack_message(payload_running , webhook)
        #add error to log
        logging.info('slack response: ' + f'{webhook_result.status_code} ' + f'{webhook_result.text}')
        logging.info('whisper model failed to load')
        logging.info(f'{e}')
        return model 
    
def s3_connect_test():
    try:
        #connect to aws S3
        s3_client = boto3.client('s3')
        s3_resource = boto3.resource('s3')
        #message to slack that AWS connection is successful 
        payload_running = {"text" : "LOCAL✅ connected to S3"}
        webhook_result = send_slack_message(payload_running , webhook)
        #add to log that connection to AWS is successful 
        logging.info('slack response: ' + f'{webhook_result.status_code} ' + f'{webhook_result.text}')
        logging.info('connected to AWS successfully')
        return s3_client, s3_resource
    except Exception as e:
        #if an error occurs print the error in the console
        print(e)
        #send a message to slack
        payload_running = {"text" : "LOCAL🛑 failed to connect to S3"}
        webhook_result = send_slack_message(payload_running , webhook)
        #log the error
        logging.info('slack response: ' + f'{webhook_result.status_code} ' + f'{webhook_result.text}')
        logging.info('failed to connect to AWS')
        logging.info(f'{e}')
