import json
import os
import boto3



s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # TODO implement
    print(event)
    bucket = event['Records'][0]['s3']['bucket']['name']
    csv_file = event['Records'][0]['s3']['object']['key']
    csv_file_obj = s3_client.get_object(Bucket=bucket, Key=csv_file)
    data = csv_file_obj['Body'].read().decode('utf-8')
    #print(data)
    
    s3_bucket = 'prod-bucket-team-e'
    key = 'raw_data/survey_results_public.csv'
    body=data
    
    s3_client.put_object(
        Bucket=s3_bucket,
        Key=key,
        Body=body
    )

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
