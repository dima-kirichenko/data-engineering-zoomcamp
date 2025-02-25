#!/usr/bin/env python
# Script to find the least frequent pickup location zone

from pyspark.sql import SparkSession
from pyspark.sql.functions import count

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Question 6 - Least Frequent Pickup Zone") \
    .getOrCreate()

# Read the taxi trip data
df_trips = spark.read.parquet("yellow_tripdata_2024-10.parquet")

# Read the zone lookup data
df_zones = spark.read.option("header", True).csv("taxi_zone_lookup.csv")

# Count pickups per zone ID
pickup_counts = df_trips.groupBy("PULocationID").agg(
    count("*").alias("pickup_count")
)

# Join with zone lookup to get zone names
zone_counts = pickup_counts.join(
    df_zones,
    pickup_counts.PULocationID == df_zones.LocationID,
    "left"
)

# Find the least frequent pickup zone
print("\nLeast frequent pickup zones:")
zone_counts.orderBy("pickup_count").select(
    "Zone",
    "pickup_count",
    "Borough"
).show(5)

spark.stop()