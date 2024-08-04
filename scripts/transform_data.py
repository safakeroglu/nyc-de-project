import pandas as pd
from google.cloud import bigquery
import logging

def clean_and_transform_data(year, month):
    try:
        file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"
        df = pd.read_parquet(f"data/{file_name}")
        
        logging.info(f"Processing data for {year}-{month:02d}")
        logging.info(f"Columns in the dataset: {df.columns.tolist()}")
        
        # Basic cleaning
        df = df.dropna()
        
        # Convert datetime columns
        datetime_columns = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']
        for col in datetime_columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Calculate trip duration
        df['trip_duration'] = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).dt.total_seconds() / 60
        
        # Apply data cleaning rules
        df = df[
            (df['trip_duration'] > 0) & (df['trip_duration'] <= 1440) &  # Max 24 hours
            (df['passenger_count'] > 0) & (df['passenger_count'] <= 8) &
            (df['trip_distance'] > 0) & (df['trip_distance'] <= 100) &
            (df['fare_amount'] > 0) &
            (df['VendorID'].isin([1, 2])) &
            (df['RatecodeID'].isin(range(1, 7))) &
            (df['payment_type'].isin(range(1, 7))) &
            (df['store_and_fwd_flag'].isin(['Y', 'N']))
        ]
        
        # Convert categorical columns
        categorical_columns = ['VendorID', 'RatecodeID', 'store_and_fwd_flag', 'payment_type', 'PULocationID', 'DOLocationID']
        for col in categorical_columns:
            df[col] = df[col].astype('category')
        
        # Save cleaned data
        output_file = f"data/cleaned_yellow_tripdata_{year}_{month:02d}.parquet"
        df.to_parquet(output_file)
        logging.info(f"Cleaned data saved to {output_file}")

    except Exception as e:
        logging.error(f"Error processing data for {year}-{month:02d}: {str(e)}")
        raise

def upload_to_bigquery(year, month):
    from google.oauth2 import service_account
    import os

    client = bigquery.Client(
        project=os.environ.get('PROJECT_ID'),
        credentials=service_account.Credentials.from_service_account_file(os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'))
    )
    dataset_id = os.environ.get('BQ_DATASET_ID')

    table_id = f"{dataset_id}.yellow_taxi_trips"
    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET)
    
    file_path = f"data/cleaned_yellow_tripdata_{year}_{month:02d}.parquet"
    with open(file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)
    job.result()
    print(f"Loaded cleaned data for {year}-{month:02d} to BigQuery")