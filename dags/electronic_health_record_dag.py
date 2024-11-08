from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from tasks.read_dataset import read_dataset
from tasks.transform_electronic_health_record import transform_electronic_health_record
from tasks.upload_s3_dataset import upload_s3_dataset
from tasks.analyse_electronic_health_record import analyse_electronic_health_record
from tasks.ml_electronic_health_record import ml_electronic_health_record
from tasks.upload_s3_analytics_plots import upload_s3_analytics_plots
import os

default_args = {
    'owner': 'Datalake Capstone Project',
    'depends_on_past': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 4, 22),
}


def create_dag(dataset_file, bucket_name, description="Sample Description", dag_id='data_processing_dag', schedule_interval=timedelta(days=1)):
    with DAG(
        dag_id,
        description,
        default_args=default_args,
        schedule_interval=schedule_interval,
        catchup=False
    ) as dag:

        read_dataset_task = PythonOperator(
            task_id='read_dataset',
            python_callable=read_dataset,
            op_kwargs={'file_path': dataset_file, 'low_memory': False}
        )

        transform_data_task = PythonOperator(
            task_id='transform_electronic_health_record',
            python_callable=transform_electronic_health_record
        )

        upload_dataset = PythonOperator(
            task_id='upload_s3_dataset',
            python_callable=upload_s3_dataset,
            op_kwargs={
                'bucket_name': bucket_name,
                'dataset_name': 'electronic-health-record-dataset',
                'dataset_path': "datasets/electronic-health-record/output/transformed_electronic_health_record_dataset.csv"
            }
        )
        
        analyze_data_task = PythonOperator(
            task_id='analyze_electronic_health_record',
            python_callable=analyse_electronic_health_record
        )
        
        ml_task = PythonOperator(
            task_id='ml_electronic_health_record',
            python_callable=ml_electronic_health_record
        )
        
        upload_analytics_plots = PythonOperator(
            task_id='upload_s3_analytics_plots',
            python_callable=upload_s3_analytics_plots,
            op_kwargs={
                'bucket_name': bucket_name,
                'folder_path': os.path.join(os.environ.get('AIRFLOW_HOME'), "analytics/electronic_health_record/insights/plots")
            }
        )
        read_dataset_task >> transform_data_task >> upload_dataset >> analyze_data_task >> upload_analytics_plots >> ml_task

    return dag

dataset_file_path = 'datasets/electronic-health-record/input/electronic_health_record_dataset.csv'
s3_bucket_name = 'deepak-sample-850072525'

dag = create_dag(
    dataset_file=dataset_file_path,
    description="This dag is for the electronic health record pipeline", 
    dag_id="electronic_health_record_dag",
    bucket_name=s3_bucket_name
)