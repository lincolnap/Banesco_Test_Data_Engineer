#!/usr/bin/env python3
"""
Divvy Bikes Data Load to PostgreSQL
Loads transformed data from MinIO staging zone to PostgreSQL analytics schema
"""

import os
import sys
import argparse
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session(app_name="DivvyBikes_LoadToPostgres"):
    """Create Spark session with MinIO and PostgreSQL configurations"""
    findspark.init()
    
    # Download JARs for S3A and PostgreSQL
    jar_paths = download_s3a_jars()
    postgres_jar = download_postgres_jar()
    jar_paths.append(postgres_jar)
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[4]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.broadcastTimeout", "36000") \
        .config("spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold", "100m") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.jars", ",".join(jar_paths)) \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{config['MINIO_ENDPOINT']}") \
        .config("spark.hadoop.fs.s3a.access.key", f"{config['MINIO_ACCESS_KEY']}") \
        .config("spark.hadoop.fs.s3a.secret.key", f"{config['MINIO_SECRET_KEY']}") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.region", "us-east-1") \
        .config("spark.hadoop.fs.s3a.fast.upload", "true") \
        .config("spark.hadoop.fs.s3a.block.size", "128m") \
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "10000") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "10000") \
        .getOrCreate()
    
    return spark

def download_s3a_jars():
    """Download S3A JAR files for MinIO connectivity"""
    import requests
    import os
    
    jar_dir = "/opt/airflow/jars"
    os.makedirs(jar_dir, exist_ok=True)
    
    jars = [
        {
            "url": "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar",
            "filename": "hadoop-aws-3.3.4.jar"
        },
        {
            "url": "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar",
            "filename": "aws-java-sdk-bundle-1.12.262.jar"
        },
        {
            "url": "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar",
            "filename": "hadoop-common-3.3.4.jar"
        }
    ]
    
    jar_paths = []
    for jar in jars:
        jar_path = os.path.join(jar_dir, jar["filename"])
        if not os.path.exists(jar_path):
            logger.info(f"Downloading {jar['filename']}...")
            response = requests.get(jar["url"])
            with open(jar_path, "wb") as f:
                f.write(response.content)
        jar_paths.append(jar_path)
    
    return jar_paths

def download_postgres_jar():
    """Download PostgreSQL JDBC driver"""
    import requests
    import os
    
    jar_dir = "/opt/airflow/jars"
    postgres_jar = os.path.join(jar_dir, "postgresql-42.6.0.jar")
    
    if not os.path.exists(postgres_jar):
        logger.info("Downloading PostgreSQL JDBC driver...")
        url = "https://jdbc.postgresql.org/download/postgresql-42.6.0.jar"
        response = requests.get(url)
        with open(postgres_jar, "wb") as f:
            f.write(response.content)
    
    return postgres_jar

def read_staging_data(spark, staging_path):
    """Read transformed data from MinIO staging zone"""
    logger.info(f"Reading data from staging path: {staging_path}")
    df = spark.read.parquet(staging_path)
    logger.info(f"Read {df.count()} records from staging zone")
    return df

def prepare_dim_stations(df):
    """Prepare dimension table for stations"""
    logger.info("Preparing dim_stations table...")
    
    # Get unique stations from start and end stations
    start_stations = df.select(
        col("start_station_id").alias("station_id"),
        col("start_station_name").alias("station_name"),
        col("start_lat").alias("latitude"),
        col("start_lng").alias("longitude")
    ).distinct()
    
    end_stations = df.select(
        col("end_station_id").alias("station_id"),
        col("end_station_name").alias("station_name"),
        col("end_lat").alias("latitude"),
        col("end_lng").alias("longitude")
    ).distinct()
    
    # Union and deduplicate
    all_stations = start_stations.union(end_stations).distinct()
    
    # Calculate station metrics
    station_metrics = df.groupBy("start_station_id").agg(
        count("*").alias("total_departures"),
        avg("trip_duration_minutes").alias("avg_departure_duration")
    )
    
    arrival_metrics = df.groupBy("end_station_id").agg(
        count("*").alias("total_arrivals"),
        avg("trip_duration_minutes").alias("avg_arrival_duration")
    )
    
    # Join with metrics
    dim_stations = all_stations.join(
        station_metrics, 
        all_stations.station_id == station_metrics.start_station_id, 
        "left"
    ).join(
        arrival_metrics,
        all_stations.station_id == arrival_metrics.end_station_id,
        "left"
    ).select(
        col("station_id"),
        col("station_name"),
        col("latitude"),
        col("longitude"),
        coalesce(col("total_departures"), lit(0)).alias("total_departures"),
        coalesce(col("total_arrivals"), lit(0)).alias("total_arrivals"),
        col("avg_departure_duration"),
        col("avg_arrival_duration")
    ).fillna(0, subset=["total_departures", "total_arrivals"])
    
    return dim_stations

def prepare_fact_rides(df):
    """Prepare fact table for rides"""
    logger.info("Preparing fact_rides table...")
    
    fact_rides = df.select(
        col("ride_id"),
        col("started_at"),
        col("ended_at"),
        col("start_station_id"),
        col("end_station_id"),
        col("member_casual"),
        col("rideable_type"),
        col("trip_duration_minutes").alias("ride_duration_minutes"),
        (col("trip_duration_minutes") / 60.0).alias("ride_duration_hours"),
        col("trip_distance_km").alias("total_distance"),
        # Calculate avg_speed_kmh if trip_duration_hours > 0
        when((col("trip_duration_minutes") / 60.0) > 0, 
             col("trip_distance_km") / (col("trip_duration_minutes") / 60.0)).otherwise(0).alias("avg_speed_kmh"),
        col("trip_distance_category").alias("distance_category"),
        col("trip_duration_category").alias("duration_category"),
        col("time_of_day"),
        col("start_hour").alias("hour_of_day"),
        col("start_day_of_week").alias("day_of_week"),
        col("start_month").alias("month"),
        col("start_year").alias("year"),
        col("dt"),
        current_timestamp().alias("created_at"),
        current_timestamp().alias("updated_at"),
        lit("divvy_bikes_api").alias("data_source"),
        lit("2025-09-29_050000").alias("batch_id"),
        lit(True).alias("is_valid"),
        current_date().alias("processing_date")
    )
    
    return fact_rides

def get_upsert_config(table_name):
    """Get upsert configuration for different tables"""
    configs = {
        "analytics.dim_time": {
            "unique_cols": ["time_key"],
            "columns": "time_key, year, month, day_of_month, day_of_week, week_of_year, quarter, is_weekend, season",
            "select_clause": "time_key::date, year, month, day_of_month, day_of_week, week_of_year, quarter, is_weekend, season",
            "update_set": "year = EXCLUDED.year, month = EXCLUDED.month, day_of_month = EXCLUDED.day_of_month, day_of_week = EXCLUDED.day_of_week, week_of_year = EXCLUDED.week_of_year, quarter = EXCLUDED.quarter, is_weekend = EXCLUDED.is_weekend, season = EXCLUDED.season"
        },
        "analytics.dim_stations": {
            "unique_cols": ["station_id"],
            "columns": "station_id, station_name, latitude, longitude, total_departures, total_arrivals, avg_departure_duration, avg_arrival_duration",
            "select_clause": "station_id, station_name, latitude, longitude, total_departures, total_arrivals, avg_departure_duration, avg_arrival_duration",
            "update_set": "station_name = EXCLUDED.station_name, latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude, total_departures = EXCLUDED.total_departures, total_arrivals = EXCLUDED.total_arrivals, avg_departure_duration = EXCLUDED.avg_departure_duration, avg_arrival_duration = EXCLUDED.avg_arrival_duration"
        },
        "analytics.fact_rides": {
            "unique_cols": ["ride_id"],
            "columns": "ride_id, started_at, ended_at, start_station_id, end_station_id, member_casual, rideable_type, ride_duration_minutes, ride_duration_hours, total_distance, avg_speed_kmh, distance_category, duration_category, time_of_day, hour_of_day, day_of_week, month, year, dt, created_at, updated_at, data_source, batch_id, is_valid, processing_date",
            "select_clause": "ride_id, started_at::timestamp, ended_at::timestamp, start_station_id, end_station_id, member_casual, rideable_type, ride_duration_minutes, ride_duration_hours, total_distance, avg_speed_kmh, distance_category, duration_category, time_of_day, hour_of_day, day_of_week, month, year, dt, created_at::timestamp, updated_at::timestamp, data_source, batch_id, is_valid, processing_date::date",
            "update_set": "started_at = EXCLUDED.started_at, ended_at = EXCLUDED.ended_at, start_station_id = EXCLUDED.start_station_id, end_station_id = EXCLUDED.end_station_id, member_casual = EXCLUDED.member_casual, rideable_type = EXCLUDED.rideable_type, ride_duration_minutes = EXCLUDED.ride_duration_minutes, ride_duration_hours = EXCLUDED.ride_duration_hours, total_distance = EXCLUDED.total_distance, avg_speed_kmh = EXCLUDED.avg_speed_kmh, distance_category = EXCLUDED.distance_category, duration_category = EXCLUDED.duration_category, time_of_day = EXCLUDED.time_of_day, hour_of_day = EXCLUDED.hour_of_day, day_of_week = EXCLUDED.day_of_week, month = EXCLUDED.month, year = EXCLUDED.year, dt = EXCLUDED.dt, updated_at = EXCLUDED.updated_at, batch_id = EXCLUDED.batch_id, is_valid = EXCLUDED.is_valid, processing_date = EXCLUDED.processing_date"
        }
    }
    return configs.get(table_name)

def execute_upsert(df_deduped, table_name, temp_table, connection_properties):
    """Execute optimized upsert operation"""
    import psycopg2
    
    conn = psycopg2.connect(
        host=connection_properties["host"],
        port=connection_properties["port"], 
        database=connection_properties["database"],
        user=connection_properties["user"],
        password=connection_properties["password"]
    )
    
    try:
        cursor = conn.cursor()
        config = get_upsert_config(table_name)
        
        if config:
            conflict_key = config["unique_cols"][0]  # Primary key
            upsert_sql = f"""
            INSERT INTO {table_name} ({config["columns"]})
            SELECT {config["select_clause"]}
            FROM {temp_table}
            ON CONFLICT ({conflict_key}) 
            DO UPDATE SET {config["update_set"]}
            """
            cursor.execute(upsert_sql)
        
        # Drop temporary table
        cursor.execute(f"DROP TABLE {temp_table}")
        conn.commit()
        
        logger.info(f"Successfully upserted {df_deduped.count()} records to {table_name}")
        
    finally:
        cursor.close()
        conn.close()

def load_to_postgres(df, table_name, connection_properties, mode="overwrite"):
    """Optimized load DataFrame to PostgreSQL table"""
    logger.info(f"Loading data to PostgreSQL table: {table_name}")
    
    try:
        # Check if table needs upsert strategy
        config = get_upsert_config(table_name)
        
        if config:
            logger.info(f"Using optimized upsert strategy for {table_name}")
            
            # Remove duplicates based on unique columns
            unique_col = config["unique_cols"][0]
            df_deduped = df.dropDuplicates([unique_col])
            
            original_count = df.count()
            deduped_count = df_deduped.count()
            logger.info(f"Removed {original_count - deduped_count} duplicate records")
            
            # Write to temporary table with optimized settings
            temp_table = f"{table_name}_temp"
            df_deduped.write \
                .mode("overwrite") \
                .option("driver", "org.postgresql.Driver") \
                .option("numPartitions", "8") \
                .option("batchsize", "20000") \
                .option("isolationLevel", "NONE") \
                .option("truncate", "true") \
                .jdbc(
                    url=connection_properties["url"],
                    table=temp_table,
                    properties=connection_properties
                )
            
            # Execute optimized upsert
            execute_upsert(df_deduped, table_name, temp_table, connection_properties)
            
        else:
            # Standard load for non-upsert tables
            df.write \
                .mode(mode) \
                .option("driver", "org.postgresql.Driver") \
                .option("numPartitions", "8") \
                .option("batchsize", "20000") \
                .option("isolationLevel", "NONE") \
                .jdbc(
                    url=connection_properties["url"],
                    table=table_name,
                    properties=connection_properties
                )
            
            logger.info(f"Successfully loaded {df.count()} records to {table_name}")
        
    except Exception as e:
        logger.error(f"Error loading data to {table_name}: {str(e)}")
        raise

def prepare_dim_time(df):
    """Prepare time dimension table"""
    logger.info("Preparing dim_time table...")
    
    # Get unique dates from the data
    unique_dates = df.select("dt").distinct()
    
    # Create time dimension with all date attributes
    dim_time = unique_dates.select(
        col("dt").alias("time_key"),
        year(col("dt")).alias("year"),
        month(col("dt")).alias("month"),
        dayofmonth(col("dt")).alias("day_of_month"),
        dayofweek(col("dt")).alias("day_of_week"),
        weekofyear(col("dt")).alias("week_of_year"),
        quarter(col("dt")).alias("quarter"),
        (dayofweek(col("dt")).isin(1, 7)).alias("is_weekend"),
        when(month(col("dt")).isin(12, 1, 2), lit("WINTER"))
        .when(month(col("dt")).isin(3, 4, 5), lit("SPRING"))
        .when(month(col("dt")).isin(6, 7, 8), lit("SUMMER"))
        .otherwise(lit("FALL")).alias("season")
    )
    
    return dim_time

def create_aggregated_tables(spark, connection_properties):
    """Create aggregated tables using SQL"""
    logger.info("Creating aggregated tables...")
    
    # Optimized daily metrics aggregation with better performance
    daily_metrics_sql = """
    INSERT INTO analytics.agg_daily_metrics (
        date_key, total_rides, total_distance, avg_distance, avg_duration_minutes,
        member_rides, casual_rides, member_avg_distance, casual_avg_distance,
        peak_hour, total_stations_used
    )
    SELECT 
        dt as date_key,
        COUNT(*) as total_rides,
        ROUND(SUM(total_distance), 3) as total_distance,
        ROUND(AVG(total_distance), 3) as avg_distance,
        ROUND(AVG(ride_duration_minutes), 2) as avg_duration_minutes,
        COUNT(*) FILTER (WHERE member_casual = 'MEMBER') as member_rides,
        COUNT(*) FILTER (WHERE member_casual = 'CASUAL') as casual_rides,
        ROUND(AVG(total_distance) FILTER (WHERE member_casual = 'MEMBER'), 3) as member_avg_distance,
        ROUND(AVG(total_distance) FILTER (WHERE member_casual = 'CASUAL'), 3) as casual_avg_distance,
        MODE() WITHIN GROUP (ORDER BY hour_of_day) as peak_hour,
        COUNT(DISTINCT start_station_id) as total_stations_used
    FROM analytics.fact_rides fr
    WHERE NOT EXISTS (
        SELECT 1 FROM analytics.agg_daily_metrics adm 
        WHERE adm.date_key = fr.dt
    )
    GROUP BY dt
    ON CONFLICT (date_key) DO NOTHING
    """
    
    # User behavior aggregation
    user_behavior_sql = """
    INSERT INTO analytics.agg_user_behavior (
        date_key, member_type, total_rides, avg_distance, avg_duration_minutes,
        total_distance, preferred_time_of_day, preferred_distance_category,
        preferred_duration_category, unique_stations_used
    )
    SELECT 
        dt as date_key,
        member_casual as member_type,
        COUNT(*) as total_rides,
        AVG(total_distance) as avg_distance,
        AVG(ride_duration_minutes) as avg_duration_minutes,
        SUM(total_distance) as total_distance,
        MODE() WITHIN GROUP (ORDER BY time_of_day) as preferred_time_of_day,
        MODE() WITHIN GROUP (ORDER BY distance_category) as preferred_distance_category,
        MODE() WITHIN GROUP (ORDER BY duration_category) as preferred_duration_category,
        COUNT(DISTINCT start_station_id) as unique_stations_used
    FROM analytics.fact_rides
    WHERE (dt, member_casual) NOT IN (
        SELECT date_key, member_type FROM analytics.agg_user_behavior
    )
    GROUP BY dt, member_casual
    ON CONFLICT (date_key, member_type) DO NOTHING
    """
    
    # Execute SQL queries using JDBC
    try:
        # Connect to PostgreSQL and execute queries
        import psycopg2
        conn = psycopg2.connect(
            host=connection_properties["host"],
            port=connection_properties["port"],
            database=connection_properties["database"],
            user=connection_properties["user"],
            password=connection_properties["password"]
        )
        
        cursor = conn.cursor()
        
        # Execute daily metrics
        logger.info("Creating daily metrics aggregation...")
        cursor.execute(daily_metrics_sql)
        conn.commit()
        
        # Execute user behavior
        logger.info("Creating user behavior aggregation...")
        cursor.execute(user_behavior_sql)
        conn.commit()
        
        cursor.close()
        conn.close()
        
        logger.info("Aggregated tables created successfully")
        
    except Exception as e:
        logger.error(f"Error creating aggregated tables: {str(e)}")
        raise

def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description='Load Divvy Bikes data to PostgreSQL')
    parser.add_argument('--year-month', required=True, help='Year-Month in format YYYY-MM')
    parser.add_argument('--minio-endpoint', required=True, help='MinIO endpoint')
    parser.add_argument('--minio-access-key', required=True, help='MinIO access key')
    parser.add_argument('--minio-secret-key', required=True, help='MinIO secret key')
    parser.add_argument('--postgres-host', default='postgres', help='PostgreSQL host')
    parser.add_argument('--postgres-port', default='5433', help='PostgreSQL port')
    parser.add_argument('--postgres-db', default='banesco_test', help='PostgreSQL database')
    parser.add_argument('--postgres-user', default='postgres', help='PostgreSQL user')
    parser.add_argument('--postgres-password', default='postgres123', help='PostgreSQL password')
    parser.add_argument('--input-bucket', default='banesco-pa-data-stage-zone', help='Input bucket name')
    parser.add_argument('--spark-master', default='local[*]', help='Spark master URL')
    
    args = parser.parse_args()
    
    # Set global config
    global config
    config = {
        'MINIO_ENDPOINT': args.minio_endpoint,
        'MINIO_ACCESS_KEY': args.minio_access_key,
        'MINIO_SECRET_KEY': args.minio_secret_key
    }
    
    try:
        # Create Spark session
        spark = create_spark_session()
        
        # Configuration
        staging_path = f"s3a://{args.input_bucket}/banes_stage_divvy_bikes/"
        
        # PostgreSQL connection properties
        connection_properties = {
            "user": args.postgres_user,
            "password": args.postgres_password,
            "driver": "org.postgresql.Driver",
            "host": args.postgres_host,
            "port": args.postgres_port,
            "database": args.postgres_db,
            "url": f"jdbc:postgresql://{args.postgres_host}:{args.postgres_port}/{args.postgres_db}"
        }
        
        # Read staging data
        df = read_staging_data(spark, staging_path)
        
        # Prepare dimension tables
        dim_stations = prepare_dim_stations(df)
        
        # Prepare fact table
        fact_rides = prepare_fact_rides(df)
        
        # Load to PostgreSQL (order matters due to foreign key constraints)
        logger.info("Loading dimension tables...")
        
        # First load dim_time to ensure all dates exist for foreign key constraints
        dim_time = prepare_dim_time(df)
        load_to_postgres(dim_time, "analytics.dim_time", connection_properties, "append")
        
        # Then load dim_stations
        load_to_postgres(dim_stations, "analytics.dim_stations", connection_properties, "append")
        
        # Finally load fact_rides (depends on both dim_time and dim_stations)
        logger.info("Loading fact table...")
        load_to_postgres(fact_rides, "analytics.fact_rides", connection_properties, "append")
        
        # Create aggregated tables
        create_aggregated_tables(spark, connection_properties)
        
        logger.info("Data load to PostgreSQL completed successfully!")
        
        # Show sample results
        logger.info("Sample data from fact_rides:")
        fact_rides.select("ride_id", "member_casual", "total_distance", "dt").show(5)
        
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        sys.exit(1)
    
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    main()
