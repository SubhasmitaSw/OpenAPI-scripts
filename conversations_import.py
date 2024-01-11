import json
import boto3
import csv
import requests
import pandas as pd
from io import StringIO
from datetime import datetime

# Intercom API Credentials 
INTERCOM_API_KEY = 'dG9rOmQyOGUxNzA2X2ZkMTBfNGM2Ml9iOTM4X2FmOWYxZjZlYWQwNToxOjA='
INTERCOM_API_ENDPOINT= 'https://api.intercom.io/'

# CIVO S3 Credentials
ACCESS_KEY = 'N0DEU6BRNKJX5XY4SDW6'
SECRET_KEY = 'r1Hg3ezGKOCmrBhYGpRpOI069cdDd4ggVJ51zzg1'
ENDPOINT = 'https://objectstore.lon1.civo.com/'
S3_BUCKET_NAME ='civo-bi-upload/experiment'

s3 = boto3.resource('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY, endpoint_url=ENDPOINT)
local_path = 'data'


def get_intercom_conversations():
    """
    This function will get all the conversations from Intercom and return a flattened json of the data
    """

    url = f'{INTERCOM_API_ENDPOINT}/conversations'

    query = {
        'per_page': 20,
        'starting_after': None,
        }

    headers = { 'Intercom-Version': '2.10',
                'Authorization': f'Bearer {INTERCOM_API_KEY}',
                'Content-Type': 'application/json',
            }
    i=0
    conversations = []
    while i<3:
        response = requests.get(url, headers=headers, params=query)
        print(response.json().get('pages', {}))
        if response.status_code!= 200:
            # print the error response from the api call
            print(f'Error getting conversations {response.status_code} {response.text}')
            return pd.DataFrame()
        
        query['starting_after'] = response.json().get('pages',{}).get('next', {}).get('starting_after', '')
        conversations = conversations + response.json().get('conversations',[])
        i+=1

    normalized_data = pd.json_normalize(conversations)
    return normalized_data



def write_to_csv(data, filename):
    keys = data[0].keys() if data else []
    with open(filename, 'w', newline='') as csvfile:

        write = csv.DictWriter(csvfile, keys)
        write.writeheader()
        write.writerows(data)

def upload_to_s3(local_file, s3_key):
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY, endpoint_url=ENDPOINT)
    s3.upload_file(local_file, S3_BUCKET_NAME, s3_key)


def main():
    conversations = get_intercom_conversations()
    if conversations is None:
        print('No conversations found')
        return
    
    print(f'Found {len(conversations)} Conversations')
    # if there is an existing file in s3 append only the new data to it else create new file
    # if s3.Object(S3_BUCKET_NAME, 'conversations.csv').get()['Body']:
    #     # get the last conversation id from the existing file
    #     last_conversation_id = s3.Object(S3_BUCKET_NAME, 'conversations.csv').get()['Body'].read().decode('utf-8').split('\n')[-2]
    #     # filter the conversations to get only the new ones
    #     new_conversations = [conversation for conversation in conversations if conversation['id'] > last_conversation_id]
    #     # if there are new conversations write them to a csv file and upload it to s3
    #     if new_conversations:
    #         write_to_csv(new_conversations, 'conversations.csv')
    #         upload_to_s3('conversations.csv', 'conversations.csv')
    # else:
    #     write_to_csv(conversations, 'conversations.csv')
    #     upload_to_s3('conversations.csv', 'conversations.csv')

    
    # filename = f'intercom_{datetime.now().strftime("%Y%m%d%H%M%S")}.csv'
    filename = "conversations.csv"

    # convert to csv and upload to s3 
    conversations.to_csv(filename, index=False)
    # upload the file to s3 or replace if there exists a file 
    upload_to_s3(conversations.to_csv(filename, index=False), filename)

    print(f'file uploaded to s3' )

    # s3_key = f'intercom_data/{filename}'

    # local_path = write_to_csv(conversations, filename)
    # local_path = conversations.to_csv(filename, index=False)

    # print(f'file downloaded' )

if __name__ == '__main__':
    main()
    




