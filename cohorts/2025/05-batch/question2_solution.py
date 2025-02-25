#!/usr/bin/env python
# Script to process Yellow October 2024 taxi data

from pyspark.sql import SparkSession
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Question 2 - Yellow October 2024") \
    .getOrCreate()

# Read the parquet file
df = spark.read.parquet("yellow_tripdata_2024-10.parquet")

# Print the initial number of partitions
print(f"Initial number of partitions: {df.rdd.getNumPartitions()}")

# Repartition the DataFrame to 4 partitions
df_repartitioned = df.repartition(4)

# Make sure the output directory doesn't exist
output_dir = "yellow_taxi_2024_10_repartitioned"
if os.path.exists(output_dir):
    import shutil
    shutil.rmtree(output_dir)

# Save the DataFrame as parquet
df_repartitioned.write.parquet(output_dir)

# Calculate the average size of the parquet files created
parquet_files = [f for f in os.listdir(output_dir) if f.endswith(".parquet")]
total_size_bytes = sum(os.path.getsize(os.path.join(output_dir, f)) for f in parquet_files)
average_size_mb = total_size_bytes / len(parquet_files) / (1024 * 1024)  # Convert bytes to MB

print(f"Number of parquet files created: {len(parquet_files)}")
print(f"Total size of all parquet files: {total_size_bytes / (1024 * 1024):.2f} MB")
print(f"Average size of each parquet file: {average_size_mb:.2f} MB")

# Stop the Spark session
spark.stop()