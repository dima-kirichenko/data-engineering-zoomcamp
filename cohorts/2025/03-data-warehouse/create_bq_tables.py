from google.cloud import bigquery
import os

# Initialize the BigQuery client
CREDENTIALS_FILE = "google_credentials.json"
client = bigquery.Client.from_service_account_json(CREDENTIALS_FILE)

# Set your project ID and dataset name
project_id = "docker-terraform-447912"  # Replace with your actual project ID
dataset_name = "ny_taxi"        # Replace with your desired dataset name

# Set your GCS bucket name (same as in load_yellow_taxi_data.py)
bucket_name = "dezoomcamp_hw3_2025_02_06"

# Create dataset if it doesn't exist
dataset_id = f"{project_id}.{dataset_name}"
dataset = bigquery.Dataset(dataset_id)
dataset.location = "US"
try:
    dataset = client.create_dataset(dataset, exists_ok=True)
    print(f"Dataset {dataset_id} created or already exists.")
except Exception as e:
    print(f"Error creating dataset: {e}")

# Create external table
external_table_id = f"{project_id}.{dataset_name}.yellow_taxi_2024_external"
external_table = bigquery.ExternalConfig("PARQUET")
external_table.source_uris = [f"gs://{bucket_name}/yellow_tripdata_2024-*.parquet"]

external_table_def = bigquery.Table(external_table_id)
external_table_def.external_data_configuration = external_table

try:
    external_table_def = client.create_table(external_table_def, exists_ok=True)
    print(f"External table {external_table_id} created or already exists.")
except Exception as e:
    print(f"Error creating external table: {e}")

# Create materialized table
materialized_table_id = f"{project_id}.{dataset_name}.yellow_taxi_2024"
sql = f"""
CREATE OR REPLACE TABLE {materialized_table_id}
AS SELECT * FROM {external_table_id}
"""

try:
    query_job = client.query(sql)
    query_job.result()  # Wait for the job to complete
    print(f"Materialized table {materialized_table_id} created successfully.")
except Exception as e:
    print(f"Error creating materialized table: {e}")