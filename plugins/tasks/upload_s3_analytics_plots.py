import boto3
from datetime import datetime, timezone
import os
import os
import boto3

def upload_folder_contents_to_s3(folder_path, bucket_name, s3_prefix=''):
    s3 = boto3.client('s3')

    for root, dirs, files in os.walk(folder_path):
        for file in files:
            local_path = os.path.join(root, file)
            
            s3_key = os.path.relpath(local_path, folder_path)
            if s3_prefix:
                s3_key = os.path.join(s3_prefix, s3_key)
            
            # s3.upload_file(local_path, bucket_name, s3_key)
            print(f"Uploaded {local_path} to S3://{bucket_name}/{s3_key}")

def upload_s3_analytics_plots(bucket_name, folder_path):
    upload_folder_contents_to_s3(folder_path=folder_path, bucket_name=bucket_name, s3_prefix="analytics/electronic_health_record/insights/plots")
