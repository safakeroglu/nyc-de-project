from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import sys
sys.path.append('/opt/airflow/scripts')
from ingest_data import download_parquet, upload_to_gcs
from transform_data import clean_and_transform_data, upload_to_bigquery
from google.oauth2 import service_account

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def get_credentials():
    return service_account.Credentials.from_service_account_file(os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'))

dag = DAG(
    'nyc_taxi_etl',
    default_args=default_args,
    description='ETL process for NYC Taxi data',
    schedule_interval=timedelta(days=1),
)

t1 = PythonOperator(
    task_id='download_parquet',
    python_callable=download_parquet,
    dag=dag,
)

t2 = PythonOperator(
    task_id='upload_to_gcs',
    python_callable=upload_to_gcs,
    op_kwargs={
        'credentials': get_credentials(),
        'bucket_name': os.environ.get('GCP_BUCKET_ID'),
        'source_file_name': 'data/yellow_tripdata_2023-01.parquet',
        'destination_blob_name': 'raw/yellow_tripdata_2023-01.parquet'
    },
    dag=dag,
)

t3 = PythonOperator(
    task_id='clean_and_transform_data',
    python_callable=clean_and_transform_data,
    dag=dag,
)

t4 = PythonOperator(
    task_id='upload_to_bigquery',
    python_callable=upload_to_bigquery,
    dag=dag,
)

t1 >> t2 >> t3 >> t4