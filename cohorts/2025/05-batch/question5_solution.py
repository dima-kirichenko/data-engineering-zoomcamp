#!/usr/bin/env python
# Script to demonstrate Spark's web UI port

from pyspark.sql import SparkSession
import time

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Question 5 - Spark UI Port") \
    .getOrCreate()

print("Spark session is running!")
print("You can access Spark UI at: http://localhost:4040")
print("The web interface shows application's dashboard including:")
print("- Jobs")
print("- Stages")
print("- Storage")
print("- Environment")
print("- Executors")

# Read some data to make sure UI has something to show
df = spark.read.parquet("yellow_tripdata_2024-10.parquet")
print("\nReading parquet file to generate some activity...")
df.count()

print("\nWaiting 30 seconds so you can check the UI...")
time.sleep(30)

# Stop the Spark session
spark.stop()