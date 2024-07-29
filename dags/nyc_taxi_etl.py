from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.append('/Users/Safak/Repos/nyc-de-project/scripts')
from ingest_data import download_parquet, upload_to_gcs
from transform_data import clean_and_transform_data, upload_to_bigquery

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

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
    op_kwargs={'bucket_name': 'your-bucket-name', 'source_file_name': 'data/yellow_tripdata_2023-01.parquet', 'destination_blob_name': 'raw/yellow_tripdata_2023-01.parquet'},
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