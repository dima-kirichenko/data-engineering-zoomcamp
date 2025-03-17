import pandas as pd
import json
import requests
import gzip
import io
import math
from time import time
from kafka import KafkaProducer
from tqdm import tqdm

# Configuration parameters
SERVER = 'localhost:9092'
TOPIC_NAME = 'green-trips'
BATCH_SIZE = 1000
CHUNK_SIZE = 100000

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

def process_and_send_data():
    try:
        print(f"Connecting to Kafka broker at {SERVER}")
        # Kafka setup
        producer = KafkaProducer(
            bootstrap_servers=[SERVER],
            value_serializer=json_serializer
        )
        
        # Check connection before proceeding
        if not producer.bootstrap_connected():
            raise ConnectionError("Failed to connect to Kafka broker")
        print("Successfully connected to Kafka broker")
            
        # Download and read the data
        url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz"
        
        print(f"Downloading data from {url}")
        response = requests.get(url)
        gzip_file = io.BytesIO(response.content)
        
        # Columns to keep
        columns_to_keep = [
            'lpep_pickup_datetime',
            'lpep_dropoff_datetime',
            'PULocationID',
            'DOLocationID',
            'passenger_count',
            'trip_distance',
            'tip_amount'
        ]
        
        print("Processing data in chunks and sending to Kafka")
        t0 = time()
        
        total_records = 0
        chunks_processed = 0
        
        with gzip.open(gzip_file, 'rb') as f:
            # Process the CSV in chunks to handle larger files
            for chunk in pd.read_csv(f, usecols=columns_to_keep, chunksize=CHUNK_SIZE):
                # Convert datetimes to strings for JSON serialization
                chunk['lpep_pickup_datetime'] = chunk['lpep_pickup_datetime'].astype(str)
                chunk['lpep_dropoff_datetime'] = chunk['lpep_dropoff_datetime'].astype(str)
                
                chunk_size = len(chunk)
                total_records += chunk_size
                chunks_processed += 1
                
                print(f"Processing chunk {chunks_processed} with {chunk_size} records")
                
                # Send in smaller batches within each chunk
                batches = math.ceil(chunk_size / BATCH_SIZE)
                
                for i in tqdm(range(batches), desc=f"Sending batch"):
                    batch_start = i * BATCH_SIZE
                    batch_end = min((i + 1) * BATCH_SIZE, chunk_size)
                    batch = chunk.iloc[batch_start:batch_end]
                    
                    for _, row in batch.iterrows():
                        message = row.to_dict()
                        producer.send(TOPIC_NAME, value=message)
                    
                    # Flush after each batch
                    producer.flush()
        
        t1 = time()
        took = t1 - t0
        print(f"Finished sending {total_records} records to Kafka")
        print(f"Time taken: {took:.2f} seconds")
        
        # Ensure proper cleanup
        producer.close()
        
    except Exception as e:
        print(f"Error: {e}")
        # Clean up resources if needed
        if 'producer' in locals():
            producer.close()

if __name__ == "__main__":
    process_and_send_data()