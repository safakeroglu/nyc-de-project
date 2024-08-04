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
    'depends_on_past': True,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_credentials():
    return service_account.Credentials.from_service_account_file(os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'))

def process_month(**kwargs):
    execution_date = kwargs['execution_date']
    year = execution_date.year
    month = execution_date.month
    
    file_name = download_parquet(year, month)
    upload_to_gcs(get_credentials(), os.environ.get('GCP_BUCKET_ID'), f"data/{file_name}", f"raw/{file_name}")
    clean_and_transform_data(year, month)
    upload_to_bigquery(year, month)

dag = DAG(
    'nyc_taxi_etl_continuous',
    default_args=default_args,
    description='ETL process for NYC Taxi data starting from 2023-01',
    schedule_interval='0 0 3 * *',  # Run monthly on the 1st day
    catchup=True,
)

process_task = PythonOperator(
    task_id='process_month',
    python_callable=process_month,
    provide_context=True,
    dag=dag,
)