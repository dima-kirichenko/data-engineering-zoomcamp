#!/bin/bash

# Set up environment variables for Spark and Java
export JAVA_HOME=/home/dima/spark/jdk-11.0.2
export SPARK_HOME=/home/dima/spark/spark-3.5.4-bin-hadoop3
export PATH=$JAVA_HOME/bin:$SPARK_HOME/bin:$PATH
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

# Run the PySpark script
python check_spark.py