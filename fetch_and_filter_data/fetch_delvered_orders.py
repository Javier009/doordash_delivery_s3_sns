import json
import boto3

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    try:
        # Read .json files from S3 bucket the just arrived
        ORIGIN_BUCKET = 'doordash-delivery-data-json-input-files'
        origin_file_name = event['Records'][0]['s3']['object']['key']
        # origin_file_name = 'mock_data_20250925-142436.json'  # For testing purposes, replace with actual event data in production
        response = s3_client.get_object(Bucket=ORIGIN_BUCKET, Key=origin_file_name)
        file_content = response['Body'].read().decode('utf-8')
        json_data = json.loads(file_content)
        delivered_orders = [order for order in json_data if order['status'] == 'delivered']

        delivered_orders_json_object = json.dumps(delivered_orders, indent=4)

        # Write to destination bucket
        DESTINATION_BUCKET = 'doordash-delvers-orders-only'
        destination_file_name = f'delivered_{origin_file_name}'
        s3_client.put_object(Bucket = DESTINATION_BUCKET,
                             Key = destination_file_name,
                            Body = delivered_orders_json_object)
        print(f"Delivered orders written to s3://{DESTINATION_BUCKET}/{destination_file_name}")
        return {
            'statusCode': 200,
            'body': json.dumps(f'Delivered orders written to s3://{DESTINATION_BUCKET}/{destination_file_name}')
        }
    except Exception as e:
        print(f"Error processing file: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error processing file: {str(e)}')
        }

# # Call the lambda handler for testing purposes
# if __name__ == "__main__":
#     lambda_handler(None, None)