import json
import pandas as pd
import boto3

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # TODO implement
    #bucket = 'doordash-landing-zn01'
    #key = '2024-03-01-raw_input.json'
    # print(event)
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if status == 200:
            print(f"Successful S3 get_object response. Status - {status}")
            file_content = response['Body']
            json_data = json.loads(file_content.read())  # reading the file content into json object.

            df = pd.json_normalize(json_data, meta=['id', 'status', 'amount', 'date'])

            df = df[df['status'] == 'delivered']
            print(df)
        else:
            print(f"Unsuccessful S3 get_object response. Status - {status}")

    except Exception as e:
        raise e

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
