from google.cloud import bigquery

# Initialize the BigQuery client
CREDENTIALS_FILE = "google_credentials.json"
client = bigquery.Client.from_service_account_json(CREDENTIALS_FILE)

# Set your project ID and dataset name
project_id = "docker-terraform-447912"
dataset_name = "ny_taxi"
table_name = "yellow_taxi_2024"

# Test different COUNT queries
queries = {
    "COUNT(*)": "SELECT COUNT(*) as total_count",
    "COUNT(1)": "SELECT COUNT(1) as total_count",
    "COUNT(VendorID)": "SELECT COUNT(VendorID) as total_count"  # Added this query
}

for description, base_query in queries.items():
    query = f"{base_query} FROM `{project_id}.{dataset_name}.{table_name}`"
    
    # Check estimated bytes
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    query_job = client.query(query, job_config=job_config)
    mb_processed = query_job.total_bytes_processed / 1024 / 1024

    print(f"\nQuery: {description}")
    print(f"Estimated bytes processed: {mb_processed:.2f} MB")