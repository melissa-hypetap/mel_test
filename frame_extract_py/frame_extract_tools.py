#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 25 10:41:35 2023

@author: melissagray
"""
import ffmpeg
import cv2
import boto3
import pymongo
from pymongo import MongoClient
import pandas as pd
import numpy as np
import uuid
from datetime import datetime
from datetime import timezone
from datetime import timedelta
import requests
import logging
import requests
import json
import os
from time import sleep
import mongo_config


#client = MongoClient("localhost", 27017)
client = MongoClient(mongo_config.address,
                      username=mongo_config.username,
                      password=mongo_config.password,
                      authSource=mongo_config.authSource,
                      authMechanism=mongo_config.authMechanism)   
db = client['hypetap']
videoToFramesQ = db["videoLabelDetectionQ"]
videoToFramesResult = db["videoFrameLabelDetectionQ"]
webhook = "https://hooks.slack.com/services/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"  




def lock_collection():
    collectionlocking = { "recordType" : "COLLECTION_LOCKING_RECORD" }
    lock = { "$set": { "status": "LOCKED", "dateLastLocked" : datetime.now(timezone.utc), "lockedBy" : "frameExtractor1"} }        #set status to LOCKED, dateLastLocked to current time in UTC, and lockedBy to be transcriber1   
    videoToFramesQ.update_one(collectionlocking, lock)  
    
def unlock_collection():
    collectionlocking = { "recordType" : "COLLECTION_LOCKING_RECORD" }  #get the collection locking record
    newvalues = { "$set": { "status": "UNLOCKED", "dateLastUnLocked" : datetime.now(timezone.utc), "lockedBy" : None} }   #  #set status to UNLOCKED, dateLastUnLocked to current time in UTC, and lockedBy to be transcriber1   
    videoToFramesQ.update_one(collectionlocking, newvalues)     
    
def get_collection_locking_record():
    collection_record = {"recordType" : "COLLECTION_LOCKING_RECORD"} #this is the only collection locking record
    result= videoToFramesQ.find(collection_record)
    collection_record_df = pd.DataFrame(list(result))
    return collection_record_df


def time_locked(collection_record_df):
    if 'dateLastLocked' in collection_record_df:
        date_last_locked = collection_record_df['dateLastLocked'][0].tz_localize('utc')
        current_time = datetime.now(timezone.utc)
        time_difference = current_time - date_last_locked
    else:
        time_difference = timedelta(minutes=20)
    return(time_difference)

def get_all_post_records():
    get_all = {"recordType" : "UPDATE_POST_RECORD"} #query entire collection **SHOULD THIS BE recordType : UPDATE_POST_RECORD ?
    ## extract query results to a python dataframe
    cursor = videoToFramesQ.find(get_all)
    df = pd.DataFrame(list(cursor)) 
    return df

def delete_record(record):
    current_record = {'_id' : record['_id']}    #get the _id of the record we have just processed
    videoToFramesQ.delete_one(current_record) 

def lock_first_record(df):
    first_record = df[df.status == 'UNLOCKED'].iloc[0]  
    get_first_record = { "_id": first_record["_id"] } # The _id of the first unlocked regord                 
    ## lock the record record
    newvalues = { "$set": { "status": "LOCKED", "dateLastLocked" : datetime.now(timezone.utc), "lockedBy" : "frameExtractor1"} }     #set status to LOCKED, dateLastLocked to current time in UTC, and lockedBy to be transcriber1 
    videoToFramesQ.update_one(get_first_record, newvalues)
    return first_record



def frame_capture(record, s3_client, filepath):
    s3_client = boto3.client('s3')
    x= record["storageId"]  #get the AWS storageId
    bucket_name = record["storageBucket"]   #get the AWS bucket name
    
    #call the single record again (removes any unwanted fields)
    get_current_record = { "_id": record["_id"] }
    single_record = videoToFramesQ.find(get_current_record)
    single_record_df = pd.DataFrame(list(single_record))
    
    
    
    s3_client.download_file(bucket_name, x, filepath)
    vidcap = cv2.VideoCapture(filepath)
    fps = int(vidcap.get(cv2.CAP_PROP_FPS))
    total_frames = int(vidcap.get(cv2.CAP_PROP_FRAME_COUNT))
    still_count = 1
    frames_data = []
    for count in range(total_frames):
        success, image = vidcap.read()
        if not success:
            break

        if count % (1 * fps) == 0:
            uuid_given = uuid.uuid4()
            timestamp_ms = int(count * 1000 / fps)
            frames_data.append({'timestamp': timestamp_ms, 'storageId': str(uuid_given), 'storageBucket' : 'hypetap-video-frames-dev' })
            still_count+=1
            name =  './results/' + str(uuid_given) + '.jpg'
            cv2.imwrite(name,image)
            s3_client.upload_file('./results/' + str(uuid_given) + '.jpg', 'hypetap-video-frames-dev', str(uuid_given),
                                ExtraArgs = {'Metadata' : {'Content-Type' : 'image/jpeg',
                                              'ilename' : str(uuid_given),
                                              'storagedatatype' : 'VIDEO_FRAME'}}

                              )
            #cv2.imwrite(name,image) #####NEED TO CHANGE TO WRITE DIRECTLY TO S3 BUCKET ONCE SET UP! client.upload_file('images/image_0.jpg', 'mybucket', 'image_0.jpg')
            with open(name, 'rb') as data:
                is_success, buffer = cv2.imencode(".jpg", image)
                image_bytes = buffer.tobytes()
            os.remove(name)   
 
    single_record_df['frames'] = [frames_data]
    cols = list(single_record_df.columns)
    cols.insert(cols.index('storageBucket') + 1, cols.pop(cols.index('frames')))
    df = single_record_df[cols]
    return df


def write_all_frames(df):
    df2 = df.replace({np.nan : None})
    df_to_list = df2.to_dict('records')
    videoToFramesResult.insert_many(df_to_list)
    
    

def send_slack_message(payload, webhook):           #define a function to send messages to slack
    return requests.post(webhook, json.dumps(payload))

                 #update the record in MongoDB    
    
    
def connect_mongo_test():
    while True:
        try:
            client = MongoClient(mongo_config.address,
                      username=mongo_config.username,
                      password=mongo_config.password,
                      authSource=mongo_config.authSource,
                      authMechanism=mongo_config.authMechanism)           
            #client = MongoClient("localhost", 27017)
            db = client['hypetap']
            videoToFramesQ = db["videoLabelDetectionQ"]
            client.server_info() # force connection on a request to verify connection
             ## DS - Do not send this webhook - instead we will rely on the exception webook since this one is being sent with every mongo connection
            return videoToFramesQ
        except pymongo.errors.ConnectionFailure as err:
            # if connection to the MongoDB database fails
                #send a message to slack
            payload = {"text": "dev🛑Connection to MongoDB refused. See error message:\n\n\n"  + str(err) + 'trying again in 10 minutes'}
            webhook_result = send_slack_message(payload, webhook)
            # add the error to the log
            logging.info('slack response: ' + f'{webhook_result.status_code} ' + f'{webhook_result.text}')   
            logging.error("Connection to MongoDB refused")
            # exit the code
            #raise Exception('Connection to MongoDB refused')
            sleep(600)

    
    
def s3_connect_test():
    try:
        #connect to aws S3
        s3_client = boto3.client('s3')
        s3_resource = boto3.resource('s3')
        #message to slack that AWS connection is successful 
        payload_running = {"text" : "dev✅ connected to S3"}
        webhook_result = send_slack_message(payload_running , webhook)
        #add to log that connection to AWS is successful 
        logging.info('slack response: ' + f'{webhook_result.status_code} ' + f'{webhook_result.text}')
        logging.info('connected to AWS successfully')
        return s3_client, s3_resource
    except Exception as e:
        #if an error occurs print the error in the console
        print(e)
        #send a message to slack
        payload_running = {"text" : "dev🛑 failed to connect to S3"}
        webhook_result = send_slack_message(payload_running , webhook)
        #log the error
        logging.info('slack response: ' + f'{webhook_result.status_code} ' + f'{webhook_result.text}')
        logging.info('failed to connect to AWS')
        logging.info(f'{e}')
