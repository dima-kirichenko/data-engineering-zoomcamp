#!/usr/bin/env python
# Script to create a local Spark session and check the Spark version

import pyspark
from pyspark.sql import SparkSession

# Create a local Spark session
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('spark_version_check') \
    .getOrCreate()

# Print the Spark version
print("Spark version:", spark.version)

# Stop the Spark session
spark.stop()