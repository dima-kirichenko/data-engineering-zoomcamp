import os
from google.cloud import bigquery

# GCP Configuration
CREDENTIALS_FILE = "/home/dima/data-engineering-zoomcamp/cohorts/2025/03-data-warehouse/google_credentials.json"
PROJECT_ID = "docker-terraform-447912"
DATASET_NAME = "raw_nyc_tripdata"
BUCKET_NAME = "dezoomcamp_hw3_2025_02_06"

# Expected row counts for validation
EXPECTED_COUNTS = {
    'green': 7_778_101,
    'yellow': 109_047_518,
    'fhv': 43_244_696
}

def create_external_tables():
    """Create external tables in BigQuery"""
    client = bigquery.Client.from_service_account_json(CREDENTIALS_FILE)
    
    # Make sure dataset exists
    dataset_ref = f"{PROJECT_ID}.{DATASET_NAME}"
    try:
        client.get_dataset(dataset_ref)
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset, exists_ok=True)
    
    for service in ["yellow", "green", "fhv"]:
        table_id = f"{PROJECT_ID}.{DATASET_NAME}.ext_{service}_taxi"
        
        # Configure external table
        external_config = bigquery.ExternalConfig("CSV")
        external_config.source_uris = [f"gs://{BUCKET_NAME}/{service}_tripdata_*.csv.gz"]
        external_config.compression = "GZIP"
        external_config.skip_leading_rows = 1
        external_config.autodetect = True
        
        # Create table definition
        table = bigquery.Table(table_id)
        table.external_data_configuration = external_config
        
        try:
            # Create or update the table
            client.delete_table(table_id, not_found_ok=True)
            client.create_table(table)
            print(f"Created external table: {table_id}")
        except Exception as e:
            print(f"Error creating {table_id}: {e}")

def verify_record_counts():
    """Verify record counts match expected values"""
    client = bigquery.Client.from_service_account_json(CREDENTIALS_FILE)
    
    for service, expected_count in EXPECTED_COUNTS.items():
        query = f"""
        SELECT COUNT(1) as cnt 
        FROM `{PROJECT_ID}.{DATASET_NAME}.ext_{service}_taxi`
        """
        
        try:
            results = client.query(query).result()
            actual_count = next(results).cnt
            
            print(f"\n{service.title()} Taxi validation:")
            print(f"Expected count: {expected_count:,}")
            print(f"Actual count: {actual_count:,}")
            print(f"Status: {'✓ PASSED' if actual_count == expected_count else '✗ FAILED'}")
        except Exception as e:
            print(f"Error querying {service} taxi data: {e}")

def main():
    print("Creating external tables in BigQuery...")
    create_external_tables()

    print("\nVerifying record counts...")
    verify_record_counts()

if __name__ == "__main__":
    main()