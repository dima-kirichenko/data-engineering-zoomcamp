from google.cloud import bigquery

# Initialize the BigQuery client
CREDENTIALS_FILE = "google_credentials.json"
client = bigquery.Client.from_service_account_json(CREDENTIALS_FILE)

# Set your project ID and dataset name
project_id = "docker-terraform-447912"
dataset_name = "ny_taxi"

# Query for distinct PULocationIDs
query = """
SELECT COUNT(DISTINCT PULocationID) as distinct_locations 
FROM `{}.{}.{}`
"""

# Check both tables
tables = ['yellow_taxi_2024_external', 'yellow_taxi_2024']

for table in tables:
    table_id = f"{project_id}.{dataset_name}.{table}"
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    
    # Run the query
    query_job = client.query(
        query.format(project_id, dataset_name, table),
        job_config=job_config
    )
    
    print(f"\nTable: {table}")
    print(f"This query will process {query_job.total_bytes_processed/1024/1024:.2f} MB when run.")
    
    # Now run the actual query to get results
    actual_query = client.query(query.format(project_id, dataset_name, table))
    results = actual_query.result()
    
    for row in results:
        print(f"Distinct PULocationIDs: {row.distinct_locations}")