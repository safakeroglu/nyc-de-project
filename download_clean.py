import os
import requests

# Create downloads folder if it doesn't exist
downloads_folder = "downloads"
os.makedirs(downloads_folder, exist_ok=True)

# URL of the Parquet file
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"

# File name
file_name = "yellow_tripdata_2023-01.parquet"

# Full path to save the file
file_path = os.path.join(downloads_folder, file_name)

# Download the file
print(f"Downloading {file_name}...")
response = requests.get(url)
response.raise_for_status()  # Raise an exception for bad status codes

# Save the file
with open(file_path, "wb") as file:
    file.write(response.content)

print(f"File downloaded and saved to: {file_path}")
