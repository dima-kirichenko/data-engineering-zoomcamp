from google.cloud import bigquery

# Initialize the BigQuery client
CREDENTIALS_FILE = "google_credentials.json"
client = bigquery.Client.from_service_account_json(CREDENTIALS_FILE)

# Set your project ID and dataset name
project_id = "docker-terraform-447912"
dataset_name = "ny_taxi"

# Query to count records with fare_amount = 0
query = f"""
SELECT COUNT(*) as zero_fare_count
FROM `{project_id}.{dataset_name}.yellow_taxi_2024`
WHERE fare_amount = 0
"""

# Run the query
try:
    query_job = client.query(query)
    results = query_job.result()
    
    for row in results:
        print(f"\nNumber of records with fare_amount = 0: {row.zero_fare_count:,}")
except Exception as e:
    print(f"Error running query: {e}")