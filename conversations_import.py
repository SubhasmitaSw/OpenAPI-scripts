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

def flatten_json(data, prefix=''):
    """
    Flatten nested JSON structure
    """
    flat_data = {}
    for key, value in data.items():
        if isinstance(value, dict):
            flat_data.update(flatten_json(value, f'{prefix}{key}_'))
        else:
            flat_data[f'{prefix}{key}'] = value
    return flat_data

def get_intercom_conversations():
    """
    This function will get all the conversations from Intercom and return a json of the data
    """
    # Get all the conversations from Intercom
    url = f'{INTERCOM_API_ENDPOINT}/conversations'

    query = {
        # get all the data from Intercom
        'per_page': 100,
        'starting_after': None,
        }

    headers = { 'Intercom-Version': '2.10',
                'Authorization': f'Bearer {INTERCOM_API_KEY}',
                'Content-Type': 'application/json',
            }
    
    # get all the conversations from Intercom in a loop and keep appending them until it comes out of the loop and then convert it alltogether to csv
    while True:
        response = requests.get(url, headers=headers, params=query)
        print(response.json())
        if response.status_code!= 200:
            # print the error response from the api call
            print(f'Error getting conversations {response.status_code} {response.text}')
            return pd.DataFrame()
        conversations = response.json().get('conversations',[])
        if not conversations:
            break
        # append all the conversation data 
        conversations.extend(response.json().get('conversations',[]))
        query['starting_after'] = response.json().get('pages',{}).get('next', {}).get('starting_after', '')


    # response = requests.get(url, headers=headers, params=query)
    # print(response.json())
    # if response.status_code!= 200:
    #     # print the error response from the api call
    #     print(f'Error getting conversations {response.status_code} {response.text}')
    #     return pd.DataFrame()
    # conversations = response.json().get('conversations',[])
    normalized_data = [flatten_json(conversations)for conversations in conversations]

    return pd.DataFrame(normalized_data)
    # return response.json().get('conversations',[])


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
    
    print(f'Found {len(conversations)} conversations')
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

    #if file is found, write it to csv and store in localqq
    filename = f'intercom_{datetime.now().strftime("%Y%m%d%H%M%S")}.csv'
    # s3_key = f'intercom_data/{filename}'

    # local_path = write_to_csv(conversations, filename)
    local_path = conversations.to_csv(filename, index=False)

    print(f'file downloaded' )

if __name__ == '__main__':
    main()
    




