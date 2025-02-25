#!/usr/bin/env python
# Script to find the longest trip duration in the dataset with data quality checks

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, round

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Question 4 - Longest Trip") \
    .getOrCreate()

# Read the parquet file
df = spark.read.parquet("yellow_tripdata_2024-10.parquet")

# Calculate trip duration in hours
df_with_duration = df.withColumn(
    "trip_duration_hours",
    round((unix_timestamp(col("tpep_dropoff_datetime")) - 
           unix_timestamp(col("tpep_pickup_datetime"))) / 3600, 2)
)

# Apply data quality filters:
# 1. Duration must be positive
# 2. Trip distance should be greater than 0
# 3. Speed should be reasonable (e.g., not too slow, indicating potential meter running)
valid_trips = df_with_duration.filter(
    (col("trip_duration_hours") > 0) & 
    (col("trip_distance") > 0) &
    (col("trip_distance") / col("trip_duration_hours") > 0.1)  # Minimum avg speed 0.1 miles/hour
)

# Find the longest trip
longest_trip = valid_trips.orderBy(col("trip_duration_hours").desc()).first()

print(f"\nLongest valid trip details:")
print(f"Duration: {longest_trip['trip_duration_hours']:.2f} hours")
print(f"Pickup time: {longest_trip['tpep_pickup_datetime']}")
print(f"Dropoff time: {longest_trip['tpep_dropoff_datetime']}")
print(f"Trip distance: {longest_trip['trip_distance']} miles")
print(f"Average speed: {longest_trip['trip_distance'] / longest_trip['trip_duration_hours']:.2f} miles/hour")

# Show top 5 longest valid trips
print("\nTop 5 longest valid trips:")
valid_trips.orderBy(col("trip_duration_hours").desc()).select(
    "trip_duration_hours",
    "trip_distance",
    col("trip_distance") / col("trip_duration_hours").alias("avg_speed_mph"),
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
).show(5)

spark.stop()