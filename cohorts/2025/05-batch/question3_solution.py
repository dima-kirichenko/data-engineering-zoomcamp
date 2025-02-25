#!/usr/bin/env python
# Script to count taxi trips that started on October 15, 2024

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, year, month, dayofmonth, hour

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Question 3 - Count October 15 Trips") \
    .getOrCreate()

# Read the parquet file
df = spark.read.parquet("yellow_tripdata_2024-10.parquet")

# Basic filter for October 15, 2024
oct_15_trips = df.filter(
    (col("tpep_pickup_datetime").isNotNull()) &
    (col("tpep_dropoff_datetime").isNotNull()) &
    (col("tpep_pickup_datetime") <= col("tpep_dropoff_datetime")) &
    (year(col("tpep_pickup_datetime")) == 2024) & 
    (month(col("tpep_pickup_datetime")) == 10) & 
    (dayofmonth(col("tpep_pickup_datetime")) == 15)
)

# Count before removing duplicates
count_with_duplicates = oct_15_trips.count()
print(f"\nNumber of trips before removing duplicates: {count_with_duplicates}")

# Remove potential duplicates by considering all fields
oct_15_trips_distinct = oct_15_trips.distinct()
count_without_duplicates = oct_15_trips_distinct.count()
print(f"Number of trips after removing duplicates: {count_without_duplicates}")

# If there's a significant difference, let's look at some duplicate examples
if count_with_duplicates != count_without_duplicates:
    print("\nExample of duplicate records:")
    oct_15_trips.groupBy(oct_15_trips.columns)\
                .count()\
                .filter(col("count") > 1)\
                .show(5)

spark.stop()