from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Duration

def create_longest_streak_sink(t_env):
    table_name = 'longest_taxi_streak'
    sink_ddl = f"""
    CREATE TABLE {table_name} (
        PULocationID INTEGER,
        DOLocationID INTEGER,
        session_duration BIGINT,
        trip_count BIGINT,
        PRIMARY KEY (PULocationID, DOLocationID) NOT ENFORCED
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

def create_events_source_kafka(t_env):
    table_name = "green_trips"
    source_ddl = f"""
    CREATE TABLE {table_name} (
        lpep_pickup_datetime TIMESTAMP(3),
        lpep_dropoff_datetime TIMESTAMP(3),
        PULocationID INTEGER,
        DOLocationID INTEGER,
        passenger_count INTEGER,
        trip_distance DOUBLE,
        tip_amount DOUBLE,
        event_watermark AS lpep_dropoff_datetime,
        WATERMARK for event_watermark as event_watermark - INTERVAL '5' SECOND
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

def session_job():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)  # Enable checkpointing every 10 seconds
    env.set_parallelism(3)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        # Create Kafka table
        source_table = create_events_source_kafka(t_env)
        longest_streak_table = create_longest_streak_sink(t_env)

        # Create a temporary view with all sessions
        t_env.execute_sql(f"""
        CREATE TEMPORARY VIEW all_sessions AS
        SELECT
            PULocationID,
            DOLocationID,
            TIMESTAMPDIFF(MINUTE, MIN(lpep_dropoff_datetime), MAX(lpep_dropoff_datetime)) AS session_duration,
            COUNT(*) AS trip_count
        FROM {source_table}
        GROUP BY 
            PULocationID,
            DOLocationID,
            SESSION(event_watermark, INTERVAL '5' MINUTES)
        """)

        # Find the overall longest session
        t_env.execute_sql(f"""
        CREATE TEMPORARY VIEW max_overall_duration AS
        SELECT 
            MAX(session_duration) AS max_duration
        FROM all_sessions;
        """)

        # Join to get the final result - the location pair with the longest streak
        t_env.execute_sql(f"""
        INSERT INTO {longest_streak_table}
        SELECT 
            a.PULocationID,
            a.DOLocationID,
            a.session_duration,
            a.trip_count
        FROM all_sessions a
        JOIN max_overall_duration m
        ON a.session_duration = m.max_duration;
        """).wait()

    except Exception as e:
        print("Session window analysis failed:", str(e))

if __name__ == '__main__':
    session_job()
