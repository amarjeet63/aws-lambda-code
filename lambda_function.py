import json
import pandas as pd
import boto3

s3 = boto3.client('s3')
sns_client = boto3.client('sns')
sns_arn = 'arn:aws:sns:ap-south-1:590184034917:doordashevents'

def lambda_handler(event, context):
    # TODO implement
    #bucket = 'doordash-landing-zn01'
    #key = '2024-03-01-raw_input.json'
    
    input_bucket_name = event['Records'][0]['s3']['bucket']['name']
    input_s3_file_key = event['Records'][0]['s3']['object']['key']
    output_bucket_name = 'doordash-target-zn01'
    output_s3_file_key = input_s3_file_key.replace("raw_input", "output")
    
    try:
        response = s3.get_object(Bucket=input_bucket_name, Key=input_s3_file_key)
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if status == 200:
            print(f"Successful S3 get_object response. Status - {status}")
            file_content = response['Body']
            json_data = json.loads(file_content.read())  # reading the file content into json object.

            df = pd.json_normalize(json_data, meta=['id', 'status', 'amount', 'date'])

            df = df[df['status'] == 'delivered']
            df = pd.DataFrame(df)

            df.to_json('s3://' + output_bucket_name + '/' + output_s3_file_key)

            message = "Input S3 File {} has been processed successfully !!".format(
                "s3://" + input_bucket_name + "/" + input_s3_file_key)
            response = sns_client.publish(Subject="Success - File processed Successfully", TargetArn=sns_arn,
                                          Message=message,
                                          MessageStructure='text')
        else:
            print(f"Unsuccessful S3 get_object response. Status - {status}")
            message = "Unsuccessful S3 get_object response. Status - "+{status}
            response = sns_client.publish(Subject="Failed - Daily Data Processing", TargetArn=sns_arn,
                                           Message=message,
                                           MessageStructure='text')

    except Exception as err:
        print(err)
        message = "Input S3 File {} has been failed to process !!".format("s3://" + input_bucket_name + "/" + input_s3_file_key)
        response = sns_client.publish(Subject="Failed - Daily Data Processing", TargetArn=sns_arn, Message=message,
                                      MessageStructure='text')

    return {
        'statusCode': 200,
        'body': json.dumps('Processing S3 files....')
    }
