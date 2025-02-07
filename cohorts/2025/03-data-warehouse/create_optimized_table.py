from google.cloud import bigquery

# Initialize the BigQuery client
CREDENTIALS_FILE = "google_credentials.json"
client = bigquery.Client.from_service_account_json(CREDENTIALS_FILE)

# Set your project ID and dataset name
project_id = "docker-terraform-447912"
dataset_name = "ny_taxi"

# Create optimized table
optimized_table_id = f"{project_id}.{dataset_name}.yellow_taxi_2024_optimized"

# SQL to create partitioned and clustered table
sql = f"""
CREATE OR REPLACE TABLE {optimized_table_id}
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS SELECT * FROM `{project_id}.{dataset_name}.yellow_taxi_2024`
"""

try:
    query_job = client.query(sql)
    query_job.result()  # Wait for the job to complete
    print(f"Created optimized table {optimized_table_id} successfully.")
    
    # Get table info to verify partitioning and clustering
    table = client.get_table(optimized_table_id)
    print(f"\nTable details:")
    print(f"Partitioned by: {table.time_partitioning.field}")
    print(f"Clustered by: {table.clustering_fields}")
    
except Exception as e:
    print(f"Error creating optimized table: {e}")