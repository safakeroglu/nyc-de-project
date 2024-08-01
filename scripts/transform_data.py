import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import os

# Constants
KEY_PATH = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
PROJECT_ID = os.environ.get('PROJECT_ID')
BQ_DATASET_ID = os.environ.get('BQ_DATASET_ID')

def clean_and_transform_data():
    df = pd.read_parquet("data/yellow_tripdata_2023-01.parquet")
    
    # Basic cleaning
    df = df.dropna()
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    
    # Calculate trip duration in minutes
    df['trip_duration'] = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).dt.total_seconds() / 60
    
    # Remove trips with negative duration or unreasonably long duration (e.g., more than 24 hours)
    df = df[(df['trip_duration'] > 0) & (df['trip_duration'] <= 1440)]
    
    # Remove trips with zero passengers or unreasonably high passenger count (e.g., more than 8)
    df = df[(df['passenger_count'] > 0) & (df['passenger_count'] <= 8)]
    
    # Remove trips with zero or negative fare amount
    df = df[df['fare_amount'] > 0]
    
    # Remove trips with unreasonably long distances (e.g., more than 100 miles)
    df = df[df['trip_distance'] <= 100]
    
    # Convert categorical columns to appropriate data types
    categorical_columns = ['VendorID', 'store_and_fwd_flag', 'payment_type', 'PULocationID', 'DOLocationID']
    if 'RatecodeID' in df.columns:
        categorical_columns.append('RatecodeID')
    else:
        print("Warning: Rate code column not found. Skipping dim_rate_code creation.")
    
    for col in categorical_columns:
        if col in df.columns:
            df[col] = df[col].astype('category')
    
    # Check for 'RateCodeID' or 'rate_code_id' column
    rate_code_column = 'RatecodeID' if 'RatecodeID' in df.columns else None

    if rate_code_column:
        # Create dim_rate_code
        dim_rate_code = df[[rate_code_column]].drop_duplicates().reset_index(drop=True)
        dim_rate_code['RateCode'] = dim_rate_code[rate_code_column].map({
            1: 'Standard rate', 2: 'JFK', 3: 'Newark', 4: 'Nassau or Westchester',
            5: 'Negotiated fare', 6: 'Group ride'
        })
        dim_rate_code = dim_rate_code.rename(columns={rate_code_column: 'RateCodeID'})
        dim_rate_code.to_parquet("data/dim_rate_code.parquet")
    else:
        print("Warning: Rate code column not found. Skipping dim_rate_code creation.")

    # Create dim_vendor
    dim_vendor = df[['VendorID']].drop_duplicates().reset_index(drop=True)
    dim_vendor['VendorName'] = dim_vendor['VendorID'].map({1: 'Creative Mobile Technologies', 2: 'VeriFone Inc'})
    dim_vendor.to_parquet("data/dim_vendor.parquet")

    # Create dim_payment_type
    dim_payment_type = df[['payment_type']].drop_duplicates().reset_index(drop=True)
    dim_payment_type = dim_payment_type.rename(columns={'payment_type': 'PaymentTypeID'})
    dim_payment_type['PaymentType'] = dim_payment_type['PaymentTypeID'].map({
        1: 'Credit card', 2: 'Cash', 3: 'No charge', 4: 'Dispute', 5: 'Unknown', 6: 'Voided trip'
    })
    dim_payment_type.to_parquet("data/dim_payment_type.parquet")

    # Update dim_location
    dim_location = df[['PULocationID', 'DOLocationID']].melt()
    dim_location = dim_location['value'].drop_duplicates().reset_index(drop=True)
    dim_location = pd.DataFrame({'LocationID': dim_location})
    dim_location['ZoneName'] = 'Unknown'  # You'll need to add actual zone names if available
    dim_location.to_parquet("data/dim_location.parquet")

    # Update fact_trip creation
    fact_trip_columns = [
        'VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count',
        'trip_distance', 'PULocationID', 'DOLocationID', 'store_and_fwd_flag',
        'payment_type', 'fare_amount', 'extra', 'mta_tax', 'improvement_surcharge', 'tip_amount',
        'tolls_amount', 'total_amount', 'congestion_surcharge', 'airport_fee'
    ]
    if rate_code_column:
        fact_trip_columns.append(rate_code_column)

    fact_trip = df[fact_trip_columns]
    fact_trip = fact_trip.rename(columns={'payment_type': 'PaymentTypeID'})
    if rate_code_column and rate_code_column != 'RateCodeID':
        fact_trip = fact_trip.rename(columns={rate_code_column: 'RateCodeID'})
    fact_trip['TripID'] = fact_trip.index
    fact_trip.to_parquet("data/fact_trip.parquet")

def upload_to_bigquery():
    client = bigquery.Client(project=PROJECT_ID, credentials=service_account.Credentials.from_service_account_file(KEY_PATH))
    dataset_id = BQ_DATASET_ID

    for table in ['dim_vendor', 'dim_rate_code', 'dim_payment_type', 'dim_location', 'fact_trip']:
        table_id = f"{dataset_id}.{table}"
        job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET)
        with open(f"data/{table}.parquet", "rb") as source_file:
            job = client.load_table_from_file(source_file, table_id, job_config=job_config)
        job.result()
        print(f"Loaded {table} to BigQuery")

if __name__ == "__main__":
    clean_and_transform_data()
    upload_to_bigquery()