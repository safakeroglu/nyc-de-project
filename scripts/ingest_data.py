import pandas as pd
from google.cloud import storage
import os

def download_parquet(year, month):
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
    df = pd.read_parquet(url)
    
    os.makedirs("data", exist_ok=True)
    
    file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"
    df.to_parquet(f"data/{file_name}")
    return file_name

def upload_to_gcs(credentials, bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client(credentials=credentials)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)