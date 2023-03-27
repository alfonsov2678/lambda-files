import json
import os
import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import random
import urllib.parse
import logging
from botocore.exceptions import ClientError
print('Loading function')

s3 = boto3.client('s3')
rekognition = boto3.client('rekognition')
REGION = 'us-east-1'
HOST = 'search-photos-xyu5nrm54xaekzmeweuqd7fjle.us-east-1.es.amazonaws.com'
INDEX = 'photos-index'

def create_index(client):
    index_name = 'photos-index'
    index_body = {
      'settings': {
        'index': {
          'number_of_shards': 1
        }
      }
    }
    response = client.indices.create(index_name, body=index_body)

def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    
    try:
        detectedLabels = rekognition.detect_labels( Image={
        'S3Object': {
            'Bucket': bucket,
            'Name': key,
        }
    })
    
        normalLabels = []
        
        for label in detectedLabels['Labels']:
            normalLabels.append(label['Name'])
        
        
        # s3 stuff
        
        s3_object = s3.head_object(Bucket=bucket, Key=key)
        
        timestamp = str(s3_object['LastModified'])
        
        try:
            customLabels = s3_object['ResponseMetadata']['HTTPHeaders']['x-amz-meta-customlabels']
            customLabels = customLabels.strip('][').split(', ')
        except Exception as e: 
            customLabels = []
        
        print(normalLabels)
        print(customLabels)
        customLabels.extend(normalLabels)
        
        print(customLabels)
        
        object_json = {
            'objectKey': key,
            'bucket': bucket,
            'createdTimestamp': timestamp,
            'labels' : customLabels
        }
       
        
        
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
           
     
    client = OpenSearch(hosts=[{
        'host': HOST,
        'port': 443
    }],
    http_auth=get_awsauth(REGION, 'es'),
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection)
      
                           
    try: 
        add_object(object_json, client, key)
    except Exception as e:
        
        # CREATE THE INDEX
        print("ADD FAILED")
        create_index(client)
        add_object(object_json, client, key)
        print("index is not created")
    
def add_object(document, client, id):
    # try adding the document
    response = client.index(
            index = "photos-index",
            body = document,
            id = id,
            refresh = True
    )
    
def get_awsauth(region, service):
    cred = boto3.Session().get_credentials()
    return AWS4Auth(cred.access_key,
                    cred.secret_key,
                    region,
                    service,
                    session_token=cred.token)
                    
