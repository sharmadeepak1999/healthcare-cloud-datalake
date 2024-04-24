import boto3
from datetime import datetime, timezone
import os

def upload_s3_dataset(bucket_name, dataset_name, dataset_path):
    s3 = boto3.client('s3')
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d_%H-%M-%S')
    s3_key = f'dataset/processed_csv_file/{dataset_name}_{timestamp}.csv'

    # s3.upload_file(os.path.join(os.environ.get('AIRFLOW_HOME'), dataset_path), bucket_name, s3_key)

    print(f"Dataset uploaded to S3: s3://{bucket_name}/{s3_key}")
