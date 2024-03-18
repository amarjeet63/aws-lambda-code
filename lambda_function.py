import json
import pandas as pd
import boto3

s3 = boto3.client('s3')


def lambda_handler(event, context):
    # TODO implement
    # bucket = 'doordash-landing-zn01'
    # key = '2024-03-01-raw_input.json'
    print(event)
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    try:
        data = s3.get_object(Bucket=bucket, Key=key)
        json_data = []
        json_data = data['Body'].read().decode('utf-8')
        df = pd.Dataframe.from_dict(json_data)
        print(json_data)

        print("Started Reading JSON file which contains multiple JSON document")

    except Exception as e:
        raise e

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
