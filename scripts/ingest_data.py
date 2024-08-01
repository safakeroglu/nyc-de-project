import pandas as pd
import pyarrow.parquet as pq
from google.cloud import storage
from google.oauth2 import service_account
import os

# Constants
KEY_PATH = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
GCP_BUCKET_ID = os.environ.get('GCP_BUCKET_ID')
PROJECT_ID = os.environ.get('PROJECT_ID')
BQ_DATASET_ID = os.environ.get('BQ_DATASET_ID')

def download_parquet():
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
    df = pd.read_parquet(url)
    
    # Create the 'data' directory if it doesn't exist
    os.makedirs("data", exist_ok=True)
    
    df.to_parquet("data/yellow_tripdata_2023-01.parquet")

def upload_to_gcs(credentials, bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client(credentials=credentials)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)

if __name__ == "__main__":
    # Create credentials object
    credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
    
    download_parquet()
    upload_to_gcs(credentials, GCP_BUCKET_ID, "data/yellow_tripdata_2023-01.parquet", "raw/yellow_tripdata_2023-01.parquet")