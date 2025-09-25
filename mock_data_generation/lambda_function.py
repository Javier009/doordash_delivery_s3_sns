import json
import random
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
    # Generate mock data
    try:
        records = random.randint(50, 150)  # Random number of records between 50 and 150
        mock_data = generate_mock_data(num_records=records)
        
        # Convert to JSON Lines format
        json_object = json.dumps(mock_data, indent=4)

        # Define S3 bucket and object key
        bucket_name = 'doordash-delivery-data-json-input-files'
        object_key = f'mock_data_{datetime.now().strftime("%Y%m%d-%H%M%S")}.json'
        
        # Upload to S3
        s3_client.put_object(Body=json_object, Bucket=bucket_name, Key=object_key)

        print(f"Mock data uploaded to s3://{bucket_name}/{object_key}")

        return {
            'statusCode': 200,
            'body': json.dumps(f'Mock data uploaded to s3://{bucket_name}/{object_key}')
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }