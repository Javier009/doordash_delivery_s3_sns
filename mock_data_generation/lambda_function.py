import json
import random
import time
from datetime import datetime 

import boto3

# Start S3 Client
s3_client = boto3.client('s3')

def generate_mock_data(num_records=100):    
    today = datetime.now().strftime("%Y%m%d-%H%M%S") 
    statuses = ["delivered", "cancelled", "order placed"]
    
    data = []
    for n in range(0, num_records+1):
        id = datetime.now().strftime("%Y%m%d-%H%M%S") + '-' + str(n)
        status = random.choice(statuses)
        amount = random.uniform(100.0, 1000.0)
        record = {
            "id": id,
            "status": status,
            "amount": round(amount, 2),
            "date": today
        }
        data.append(record)
    
    return data
    
def lambda_handler(event, context):
     # Generate mock data and upload to S3
    number_of_files = random.randint(2, 10)
    print(f"Generating {number_of_files} mock data files")
    try:
        for i in range(1, number_of_files + 1):
            records = random.randint(50, 150)  # Random number of records between 50 and 150
            mock_data = generate_mock_data(num_records=records)
            
            # Convert to JSON Lines format
            json_object = json.dumps(mock_data, indent=4)

            # Define S3 bucket and object key
            bucket_name = 'doordash-delivery-data-json-input-files'
            object_key = f'mock_data_{datetime.now().strftime("%Y%m%d-%H%M%S")}.json'
            
            # Upload to S3
            s3_client.put_object(Body=json_object, Bucket=bucket_name, Key=object_key)

            print(f"File numner {i}: Mock data uploaded to s3://{bucket_name}/{object_key}")

            time.sleep(5)  # Ensure unique timestamps for filenames

        return {
            'statusCode': 200,
            'body': json.dumps(f'Mock data uploaded to s3://{bucket_name}/{object_key}')
            }
    
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }