from google.cloud import bigquery

# Initialize the BigQuery client
CREDENTIALS_FILE = "google_credentials.json"
client = bigquery.Client.from_service_account_json(CREDENTIALS_FILE)

# Set your project ID and dataset name
project_id = "docker-terraform-447912"
dataset_name = "ny_taxi"

# Query template
query_template = """
SELECT DISTINCT VendorID
FROM `{}.{}.{}`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15'
"""

# Tables to test
tables = {
    "Non-partitioned": "yellow_taxi_2024",
    "Partitioned": "yellow_taxi_2024_optimized"
}

# Test each table
for description, table in tables.items():
    # Create dry run config
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    
    # Run the query
    query = query_template.format(project_id, dataset_name, table)
    query_job = client.query(query, job_config=job_config)
    
    # Get the bytes that would be processed
    mb_processed = query_job.total_bytes_processed / 1024 / 1024
    
    print(f"\n{description} table:")
    print(f"Estimated bytes processed: {mb_processed:.2f} MB")
    
    # Run actual query to get results
    actual_query = client.query(query)
    results = actual_query.result()
    
    # Convert results to list for display
    vendor_ids = [row.VendorID for row in results]
    print(f"Distinct VendorIDs found: {vendor_ids}")