from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Duration

def create_session_results_sink(t_env):
    table_name = 'taxi_trip_sessions'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            pu_location_id INT,
            do_location_id INT,
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            trip_count BIGINT,
            session_duration_minutes DOUBLE,
            PRIMARY KEY (pu_location_id, do_location_id, session_start) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name

def create_green_trips_source(t_env):
    table_name = "green_trips"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime TIMESTAMP(3),
            lpep_dropoff_datetime TIMESTAMP(3),
            PULocationID INT,
            DOLocationID INT,
            passenger_count INT,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            dropoff_ts AS lpep_dropoff_datetime,
            WATERMARK FOR dropoff_ts AS dropoff_ts - INTERVAL '5' SECONDS
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name

def session_analysis():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(3)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    
    try:
        # Create tables
        source_table = create_green_trips_source(t_env)
        sink_table = create_session_results_sink(t_env)

        # Execute SQL with session window
        t_env.execute_sql(f"""
        INSERT INTO {sink_table}
        SELECT
            PULocationID AS pu_location_id,
            DOLocationID AS do_location_id,
            SESSION_START(dropoff_ts, INTERVAL '5' MINUTES) AS session_start,
            SESSION_END(dropoff_ts, INTERVAL '5' MINUTES) AS session_end,
            COUNT(*) AS trip_count,
            TIMESTAMPDIFF(MINUTE, 
                          SESSION_START(dropoff_ts, INTERVAL '5' MINUTES), 
                          SESSION_END(dropoff_ts, INTERVAL '5' MINUTES)) AS session_duration_minutes
        FROM {source_table}
        GROUP BY 
            SESSION(dropoff_ts, INTERVAL '5' MINUTES),
            PULocationID, 
            DOLocationID;
        """).wait()

    except Exception as e:
        print("Session analysis failed:", str(e))

if __name__ == '__main__':
    session_analysis()