import json
import os
import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import random
import urllib.parse
import logging
import inflection
from botocore.exceptions import ClientError

REGION = 'us-east-1'
HOST = 'search-photos-xyu5nrm54xaekzmeweuqd7fjle.us-east-1.es.amazonaws.com'
INDEX = 'photos-index'


client = boto3.client('lexv2-runtime')


def lambda_handler(event, context):
    print(event)
    print(context)
    query = event['queryStringParameters']["q"]
    
    # get the utterances from Lex
    send_lex_message = client.recognize_text(
     botAliasId="TSTALIASID",
     botId="GCLDLOWI26",
     localeId="en_US",
     text=query,
     sessionId='214763411219626'
    )

    intent = send_lex_message['sessionState']['intent']
    slots = intent['slots']

    try:
        keyWordOneObject = slots['KeyWordOne']
    except Exception as e: 
        keyWordOneObject = None
        
    try:
        keyWordTwoObject = slots['KeyWordTwo']
    except Exception as e: 
        keyWordTwoObject = None
        
    
    keyWordList = []

    if keyWordOneObject is not None:
        keyWordList.append(keyWordOneObject['value']['interpretedValue'])
        
        word = keyWordOneObject['value']['interpretedValue']
        
        # append the plural or not plural version
        if inflection.pluralize(word) != word:
            keyWordList.append(inflection.pluralize(word))
        if inflection.singularize(word) != word:
            keyWordList.append(inflection.singularize(word))
        
    if keyWordTwoObject is not None:
        keyWordList.append(keyWordTwoObject['value']['interpretedValue'])
        
        word = keyWordTwoObject['value']['interpretedValue']
        
        # append the plural or not plural version
        if inflection.pluralize(word) != word:
            keyWordList.append(inflection.pluralize(word))
        if inflection.singularize(word) != word:
            keyWordList.append(inflection.singularize(word))
    print(keyWordList)
    
    # SEARCH FOR EACH KEYWORD FOUND
    resultsTotal = []
    bucket_name = 'photos-concierge-s3b2ooo2-gn4y6jcf4bfh'

    for keyword in keyWordList:
        term = {'labels': keyword}
            
        results = query_search(keyword)
        
        # generate presigned URLs
        for result in results:
            resultsTotal.append(create_presigned_url(result['bucket'], result['objectKey']))
        
        
        
    return {
                'statusCode': 200,
                'headers': {
                    'Access-Control-Allow-Headers': 'Content-Type',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'GET, OP',
                },
                'body': json.dumps({'results': resultsTotal})
            }
 
def create_presigned_url(bucket_name, object_name, expiration=3600):
    """Generate a presigned URL to share an S3 object

    :param bucket_name: string
    :param object_name: string
    :param expiration: Time in seconds for the presigned URL to remain valid
    :return: Presigned URL as string. If error, returns None.
    """

    # Generate a presigned URL for the S3 object
    s3_client = boto3.client('s3')
    try:
        response = s3_client.generate_presigned_url('get_object',
                                                    Params={'Bucket': bucket_name,
                                                            'Key': object_name},
                                                    ExpiresIn=expiration)
    except ClientError as e:
        logging.error(e)
        return None

    # The response contains the presigned URL
    return response
   
def query_search(term):
    q = {'size': 1000, 'query': {'multi_match': {'query': term}}}
    client = OpenSearch(hosts=[{
        'host': HOST,
        'port': 443
    }],
    http_auth=get_awsauth(REGION, 'es'),
   use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection)
    res = client.search(index=INDEX, body=q)
    hits = res['hits']['hits']
    results = []
    for hit in hits:
        results.append(hit['_source'])
    return results
    
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
                    
