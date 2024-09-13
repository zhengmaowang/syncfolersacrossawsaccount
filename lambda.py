import json
import boto3
import os
from botocore.exceptions import NoCredentialsError, ClientError

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Source and destination bucket info
    source_bucket = os.environ['SOURCE_BUCKET']
    destination_bucket = os.environ['DESTINATION_BUCKET']
    folder_prefix = os.environ['FOLDER_PREFIX']  # e.g., "myfolder/"
    
    try:
        # List objects in the folder
        response = s3.list_objects_v2(Bucket=source_bucket, Prefix=folder_prefix)
        
        if 'Contents' not in response:
            return {
                'statusCode': 404,
                'body': json.dumps(f'No files found in folder: {folder_prefix}')
            }

        # Iterate through the objects in the folder
        for obj in response['Contents']:
            source_key = obj['Key']
            
            # Ensure that we are only copying files within the specified folder
            if not source_key.endswith('/'):  # Exclude any "folder" markers
                copy_source = {
                    'Bucket': source_bucket,
                    'Key': source_key
                }
                
                # The destination key will be the same as the source key
                destination_key = source_key
                
                print(f'Copying {source_key} from {source_bucket} to {destination_bucket}')
                
                # Copy the file to the destination bucket
                s3.copy_object(CopySource=copy_source, Bucket=destination_bucket, Key=destination_key)
                
        return {
            'statusCode': 200,
            'body': json.dumps('Files copied successfully')
        }
    
    except NoCredentialsError as e:
        print(f'Credentials error: {e}')
        return {
            'statusCode': 403,
            'body': json.dumps('Permission error')
        }
    except ClientError as e:
        print(f'Client error: {e}')
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error occurred: {e}')
        }
