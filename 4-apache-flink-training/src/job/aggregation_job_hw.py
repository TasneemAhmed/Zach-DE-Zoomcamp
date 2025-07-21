"""
Script Overview
---------------
This script (`aggregation_job_hw.py`) sets up and runs an Apache Flink streaming aggregation job using PyFlink.
It reads processed web traffic events from a Kafka topic, aggregates the number of hits per IP and host in 5-minute session windows,
and writes the aggregated results to a PostgreSQL database.

Main Components:
- Kafka Source Table: Reads processed web traffic events from a Kafka topic.
- Aggregation Logic: Groups events by IP and host in 5-minute session windows and counts hits.
- Postgres Sink Table: Writes aggregated results to a PostgreSQL table.
- Flink Job Execution: The job is started in the `log_aggregation()` function, which sets up the environment, creates the tables, and executes the aggregation pipeline.

Environment Variables Required:
- KAFKA_URL: Kafka bootstrap servers
- KAFKA_TOPIC: Kafka topic name
- KAFKA_GROUP: Kafka consumer group ID
- KAFKA_WEB_TRAFFIC_KEY: Kafka SASL username
- KAFKA_WEB_TRAFFIC_SECRET: Kafka SASL password
- POSTGRES_URL: JDBC URL for PostgreSQL
- POSTGRES_USER: PostgreSQL username
- POSTGRES_PASSWORD: PostgreSQL password

How to Run This Script
----------------------
    make sure to have these files in the same directory:
    Dockerfile, docker-compose.yml, flink-env.env, requirements.txt, makefile
    The commands to run the script are:
        1. make sure to run start_job.py first
        2. make up
        3. make aggregation_job_hw


"""
import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment
from pyflink.table.expressions import lit, col
from pyflink.table.window import Session


def create_aggregated_events_sink_postgres(t_env):
    """
    Creates a JDBC sink table in Flink for aggregated processed events mapped to a PostgreSQL table.

    Args:
        t_env (StreamTableEnvironment): The Flink table environment.

    Returns:
        str: The name of the created Postgres sink table.
    Note:
        The table must exist in the PostgreSQL database before running this job.
    """
    table_name = 'processed_events_aggregated_host'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            event_hour TIMESTAMP(3),
            ip VARCHAR,
            host VARCHAR,
            count_hits numeric
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_processed_events_source_kafka(t_env):
    """
    Creates a Kafka source table in Flink for processed web traffic events.

    Args:
        t_env (StreamTableEnvironment): The Flink table environment.

    Returns:
        str: The name of the created Kafka source table.
    """
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    table_name = "process_events_kafka"
    pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            event_time VARCHAR,
            referrer VARCHAR,
            host VARCHAR,
            url VARCHAR,
            geodata VARCHAR,
            window_timestamp AS TO_TIMESTAMP(event_time, '{pattern}'),
            WATERMARK FOR window_timestamp AS window_timestamp - INTERVAL '15' SECOND
        ) WITH (
             'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_TOPIC')}',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_key}\" password=\"{kafka_secret}\";',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def log_aggregation():
    """
    Main function to set up the Flink streaming aggregation job, create source/sink tables,
    and execute the data pipeline to aggregate web traffic events.

    Steps:
        - Initializes Flink streaming and table environments.
        - Creates Kafka source and Postgres sink tables.
        - Executes SQL statements to aggregate events by IP and host in 5-minute session windows.
        - Writes aggregated results to PostgreSQL.
    """
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    #automate checkpointing every 10 seconds
    env.enable_checkpointing(10 * 1000)
    #every operator(in this script Source: process_events_kafka & GroupWindowAggregate processed_events_aggregated_host) will run in 3 parallel instances
    env.set_parallelism(3) 
    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        """Create Kafka table
        #➡️ It tells Flink:
            “From now on, whenever I refer to process_events_kafka table in this job, 
            I'm actually referring to this Kafka topic.”
            not create kafka topic
        It’s just a logical mapping (think of it like a SQL view, or CREATE EXTERNAL TABLE in Hive).
"""        
        source_table = create_processed_events_source_kafka(t_env)

        aggregated_table = create_aggregated_events_sink_postgres(t_env)
        #Starts consuming from the topic
        # make a group by on host and every 5 minutes based on event_time

        t_env.from_path(source_table)\
            .window(
            Session.with_gap(lit(5).minutes).on(col("window_timestamp")).alias("w")
        ).group_by(
            col("w"),
            col("ip"),
            col("host")
        ) \
            .select(
                    col("w").start.alias("event_hour"),
                    col("ip"),
                    col("host"),
                    col("host").count.alias("count_hits")
            ) \
            .execute_insert(aggregated_table)


    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    log_aggregation()


