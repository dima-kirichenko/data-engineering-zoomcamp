from google.cloud import bigquery

# Initialize the BigQuery client
CREDENTIALS_FILE = "google_credentials.json"
client = bigquery.Client.from_service_account_json(CREDENTIALS_FILE)

# Set your project ID and dataset name
project_id = "docker-terraform-447912"
dataset_name = "ny_taxi"
table_name = "yellow_taxi_2024"  # Using the materialized table

# Query 1: Only PULocationID
query1 = f"""
SELECT PULocationID 
FROM `{project_id}.{dataset_name}.{table_name}`
"""

# Query 2: Both PULocationID and DOLocationID
query2 = f"""
SELECT PULocationID, DOLocationID 
FROM `{project_id}.{dataset_name}.{table_name}`
"""

# Function to run query and get estimated bytes
def check_query_bytes(query, description):
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    query_job = client.query(query, job_config=job_config)
    mb_processed = query_job.total_bytes_processed / 1024 / 1024
    print(f"\n{description}")
    print(f"This query will process {mb_processed:.2f} MB when run.")

# Run both queries and compare
print("Comparing estimated bytes processed:")
check_query_bytes(query1, "Query 1: Selecting only PULocationID")
check_query_bytes(query2, "Query 2: Selecting PULocationID and DOLocationID")