import os
import pandas as pd
import logging

def read_dataset(file_path, ti, low_memory=True):
    try:
        airflow_home = os.environ.get('AIRFLOW_HOME')
        dataset_file_path = os.path.join(airflow_home, file_path)
        df = pd.read_csv(dataset_file_path, low_memory=low_memory)
        ti.xcom_push(key='dataset', value=df)

        logging.info(f"Dataset read successfully, file: {file_path}")
    except Exception as e:
        logging.error(f"Error reading dataset: {str(e)}")
        raise
