from google.cloud import bigquery

# Initialize the BigQuery client
CREDENTIALS_FILE = "google_credentials.json"
client = bigquery.Client.from_service_account_json(CREDENTIALS_FILE)

# Set your project ID and dataset name
project_id = "docker-terraform-447912"
dataset_name = "ny_taxi"

def check_table_info(table_id):
    try:
        table = client.get_table(table_id)
        print(f"\nTable: {table_id}")
        print(f"Table type: {'External' if table.external_data_configuration else 'Materialized'}")
        print(f"Created: {table.created}")
        print(f"Rows: {table.num_rows}")
        print(f"Size: {table.num_bytes / 1024 / 1024:.2f} MB")
        
        # Show schema
        print("\nSchema:")
        for field in table.schema:
            print(f"- {field.name}: {field.field_type}")
            
        if table.external_data_configuration:
            print(f"\nExternal data source: {table.external_data_configuration.source_uris}")
            
    except Exception as e:
        print(f"Error getting table info: {e}")

# Check both tables
external_table_id = f"{project_id}.{dataset_name}.yellow_taxi_2024_external"
materialized_table_id = f"{project_id}.{dataset_name}.yellow_taxi_2024"

print("Checking tables in BigQuery...")
check_table_info(external_table_id)
check_table_info(materialized_table_id)

# Run a simple count query on both tables to verify data
for table_id in [external_table_id, materialized_table_id]:
    query = f"SELECT COUNT(*) as count FROM `{table_id}`"
    try:
        query_job = client.query(query)
        results = query_job.result()
        for row in results:
            print(f"\nTotal records in {table_id}: {row.count:,}")
    except Exception as e:
        print(f"Error running count query on {table_id}: {e}")