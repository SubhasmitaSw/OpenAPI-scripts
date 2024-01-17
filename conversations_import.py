import boto3
import csv
import requests
import pandas as pd
import io 
import datetime
from dotenv import load_dotenv
import os

load_dotenv()

# API & S3 Credentials from .env file

INTERCOM_API_KEY = os.getenv('INTERCOM_API_KEY')
INTERCOM_API_ENDPOINT = os.getenv('INTERCOM_API_ENDPOINT')
ACCESS_KEY = os.getenv('ACCESS_KEY')
SECRET_KEY = os.getenv('SECRET_KEY')
ENDPOINT = os.getenv('ENDPOINT')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

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
    while True:
        response = requests.get(url, headers=headers, params=query)
        print(response.json().get('pages', {}))
        if response.status_code!= 200:
            # print the error response from the api call
            print(f'Error getting conversations {response.status_code} {response.text}')
            return pd.DataFrame()
        # conversations = response.json().get('conversations',[])
        
        if i == 1:
            break
        query['starting_after'] = response.json().get('pages',{}).get('next', {}).get('starting_after', '')
        conversations = conversations + response.json().get('conversations',[])

        if not response.json().get('pages',{}).get('next', {}).get('starting_after', ''):
            break
        i+=1


    # convert the json date columns to datetime
    for conversation in conversations:
        conversation['created_at'] = datetime.datetime.utcfromtimestamp(conversation['created_at']).strftime('%Y-%m-%d %H:%M:%S')
        conversation['updated_at'] = datetime.datetime.utcfromtimestamp(conversation['updated_at']).strftime('%Y-%m-%d %H:%M:%S')
        conversation['first_contact_reply']['created_at'] = datetime.datetime.utcfromtimestamp(conversation['first_contact_reply']['created_at']).strftime('%Y-%m-%d %H:%M:%S')
        conversation['statistics']['first_contact_reply_at'] = datetime.datetime.utcfromtimestamp(conversation['statistics']['first_contact_reply_at']).strftime('%Y-%m-%d %H:%M:%S')
        conversation['statistics']['last_close_at'] = datetime.datetime.utcfromtimestamp(conversation['statistics']['last_close_at']).strftime('%Y-%m-%d %H:%M:%S')


    normalized_data = pd.json_normalize(conversations)

    # drop the following columns from the data 
    normalized_data = normalized_data.drop(columns=['type', 'sla_applied', 'source.body', 'source.attachments', 
                                                    'contacts.contacts', 'tags.type', 'tags.tags', 
                                                    'teammates.type', 'teammates.admins', 'custom_attributes.Language', 
                                                    'linked_objects.data', 'linked_objects.total_count', 'linked_objects.has_more', 
                                                    'conversation_rating.created_at',
                                                    'conversation_rating.contact.type',
                                                    'conversation_rating.contact.id',
                                                    'conversation_rating.contact.external_id',
                                                    'conversation_rating.teammate.type',
                                                    'conversation_rating.teammate.id',])

    return normalized_data


# def upload_to_s3(local_file, s3_key):
#     s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY, endpoint_url=ENDPOINT)
#     s3.upload_file(local_file, S3_BUCKET_NAME, s3_key)

def upload_to_s3_from_ram(data, filename):
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY, endpoint_url=ENDPOINT)
    s3.upload_fileobj(data, Bucket=S3_BUCKET_NAME, Key=filename)


def main():
    conversations = get_intercom_conversations()
    if conversations is None:
        print('No conversations found')
        return
    
    print(f'Found {len(conversations)} Conversations')

    
    # filename = f'intercom_{datetime.now().strftime("%Y%m%d%H%M%S")}.csv'
    filename = "conversation.csv"

    # convert to csv and upload to s3 
    conversations.to_csv("conversations.csv", index=False)
    # upload the file to s3 or replace if there exists a file 
    # upload_to_s3("./conversations.csv", filename)

    files = io.BytesIO(conversations.to_csv(index=False).encode('utf-8'))
    # upload to s3 directly from RAM instead of from disk
    # upload_to_s3_from_ram(files, filename)

    print(f'file uploaded to s3' )

if __name__ == '__main__':
    main()
    




