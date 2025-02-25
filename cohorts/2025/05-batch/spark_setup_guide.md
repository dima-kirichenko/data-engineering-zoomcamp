# Setting up persistent Spark and Java environment variables in WSL

To make your Spark environment settings persistent across WSL restarts, you need to add the environment variables to your shell profile file.

## Instructions:

1. Open your `.bashrc` file using a text editor:
   ```bash
   nano ~/.bashrc
   ```

2. Add the following lines at the end of the file:
   ```bash
   # Spark and Java environment variables
   export JAVA_HOME=/home/dima/spark/jdk-11.0.2
   export SPARK_HOME=/home/dima/spark/spark-3.5.4-bin-hadoop3
   export PATH=$JAVA_HOME/bin:$SPARK_HOME/bin:$PATH
   export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH
   ```

3. Save the file and exit the editor.
   - In nano: Press `Ctrl+O` to save, then `Enter`, then `Ctrl+X` to exit.

4. Apply the changes immediately:
   ```bash
   source ~/.bashrc
   ```

Now your Spark environment will be properly configured every time you start your WSL session.

## Testing the Configuration

After restarting WSL, you can verify that your environment is correctly set up by running:
```bash
echo $SPARK_HOME
echo $JAVA_HOME
python /home/dima/data-engineering-zoomcamp/cohorts/2025/05-batch/check_spark.py
```

This should display the paths to your Spark and Java installations, and then run the Spark session check script.