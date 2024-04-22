import boto3
import os

def upload_to_s3(bucket_name, local_path, s3_key):
    s3 = boto3.client('s3')
    try:
        s3.upload_file(local_path, bucket_name, s3_key)
        s3_url = f"https://{bucket_name}.s3.amazonaws.com/{s3_key}"
        return s3_url
    except Exception as e:
        print(f"Error uploading file to S3: {e}")
        return None

def upload_s3_medical_imaging_scans(bucket_name, ti):
    df = ti.xcom_pull(task_ids='transform_medical_imaging', key='transformed_dataset')

    # for index, row in df.iterrows():
    #     s3_key = f"{row['folder']}/{row['filename']}"
    #     try:
    #         s3_url = upload_to_s3(bucket_name, row['image_local_path'], s3_key)
    #     except:
    #         s3_url = None
    #     df.at[index, 's3_url'] = s3_url

    df.to_csv(os.path.join(os.environ.get('AIRFLOW_HOME'), "datasets/medical-imaging/output/final_medical_imaging_dataset.csv"), index=False)
    ti.xcom_push(key='final_dataset', value=df)