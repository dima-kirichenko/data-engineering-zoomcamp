import pandas as pd
import json
import requests
import gzip
import io
from time import time
from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

# Kafka setup
server = 'localhost:9092'
topic_name = 'green-trips'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

# Download and read the data
url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz"

print(f"Downloading data from {url}")
response = requests.get(url)
gzip_file = io.BytesIO(response.content)

# Read the data into a pandas DataFrame and keep only the required columns
columns_to_keep = [
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'tip_amount'
]

print("Reading data into DataFrame")
with gzip.open(gzip_file, 'rb') as f:
    df = pd.read_csv(f, usecols=columns_to_keep)

# Convert datetimes to strings to make them JSON serializable
df['lpep_pickup_datetime'] = df['lpep_pickup_datetime'].astype(str)
df['lpep_dropoff_datetime'] = df['lpep_dropoff_datetime'].astype(str)

print(f"Total records to send: {len(df)}")

# Send data to Kafka
print("Starting to send data to Kafka")
t0 = time()

for _, row in df.iterrows():
    message = row.to_dict()
    producer.send(topic_name, value=message)

# Flush to make sure all messages are sent
producer.flush()

t1 = time()
took = t1 - t0
print(f"Finished sending data to Kafka")
print(f"Time taken: {took:.2f} seconds")
