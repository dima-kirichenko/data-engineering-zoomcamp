import os
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
import time

# Change this to your bucket name
BUCKET_NAME = "dezoomcamp_hw3_2025_02_06"

# If you authenticated through the GCP SDK you can comment out these two lines
CREDENTIALS_FILE = "/home/dima/data-engineering-zoomcamp/cohorts/2025/03-data-warehouse/google_credentials.json"
client = storage.Client.from_service_account_json(CREDENTIALS_FILE)

DOWNLOAD_DIR = "."
CHUNK_SIZE = 8 * 1024 * 1024

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
bucket = client.bucket(BUCKET_NAME)

# Define the data sources
YELLOW_BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_"
GREEN_BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_"
FHV_BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_"

def generate_date_ranges():
    """Generate the date ranges for each taxi type"""
    yellow_green_dates = [(year, month) for year in [2019, 2020] 
                         for month in range(1, 13)]
    fhv_dates = [(2019, month) for month in range(1, 13)]
    return {
        "yellow": yellow_green_dates,
        "green": yellow_green_dates,
        "fhv": fhv_dates
    }

def download_file(params):
    """Download a single taxi data file"""
    service_type, year, month = params
    base_urls = {
        "yellow": YELLOW_BASE_URL,
        "green": GREEN_BASE_URL,
        "fhv": FHV_BASE_URL
    }
    
    url = f"{base_urls[service_type]}{year}-{month:02d}.csv.gz"
    file_path = os.path.join(DOWNLOAD_DIR, f"{service_type}_tripdata_{year}-{month:02d}.csv.gz")

    try:
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, file_path)
        print(f"Downloaded: {file_path}")
        return file_path
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None

def verify_gcs_upload(blob_name):
    """Verify that a file was uploaded to GCS successfully"""
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)

def upload_to_gcs(file_path, max_retries=3):
    """Upload a file to GCS with retries"""
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_path} to {BUCKET_NAME} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")

            if verify_gcs_upload(blob_name):
                print(f"Verification successful for {blob_name}")
                # Clean up local file after successful upload
                os.remove(file_path)
                return True
            else:
                print(f"Verification failed for {blob_name}, retrying...")
        except Exception as e:
            print(f"Failed to upload {file_path} to GCS: {e}")

        time.sleep(5)

    print(f"Giving up on {file_path} after {max_retries} attempts.")
    return False

def process_taxi_type(service_type):
    """Process all files for a specific taxi type"""
    date_ranges = generate_date_ranges()[service_type]
    params = [(service_type, year, month) for year, month in date_ranges]
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        file_paths = list(executor.map(download_file, params))

    # Filter out None values (failed downloads)
    file_paths = [f for f in file_paths if f]

    with ThreadPoolExecutor(max_workers=4) as executor:
        upload_results = list(executor.map(upload_to_gcs, file_paths))

    return len([r for r in upload_results if r])

def main():
    """Main entry point for the script"""
    taxi_types = ["yellow", "green", "fhv"]
    total_success = 0

    for taxi_type in taxi_types:
        print(f"\nProcessing {taxi_type} taxi data...")
        success_count = process_taxi_type(taxi_type)
        total_success += success_count
        print(f"Completed {taxi_type} taxi data: {success_count} files processed successfully")

    print(f"\nAll taxi types processed. Total successful uploads: {total_success}")

if __name__ == "__main__":
    main()