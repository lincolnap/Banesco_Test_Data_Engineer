"""
Divvy Bikes Data Transformation - Airflow Compatible
Read Parquet from MinIO Raw Zone → Transform Data → Save to Staging Zone
"""

import os
import sys
import argparse
import findspark
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
from datetime import datetime


def get_config_from_args():
    """Get configuration from command line arguments (passed by Airflow)"""
    parser = argparse.ArgumentParser(description='Divvy Bikes Data Transformation')
    parser.add_argument('--year-month', required=True, help='Year and month in YYYYMM format')
    parser.add_argument('--minio-endpoint', default='minio:9000', help='MinIO endpoint')
    parser.add_argument('--minio-access-key', default='minioadmin', help='MinIO access key')
    parser.add_argument('--minio-secret-key', default='minioadmin123', help='MinIO secret key')
    parser.add_argument('--spark-master', default='spark://spark-master:7077', help='Spark master URL')
    parser.add_argument('--input-bucket', default='banesco-pa-data-raw-zone', help='Input bucket name')
    parser.add_argument('--output-bucket', default='banesco-pa-data-stage-zone', help='Output bucket name')
    
    args = parser.parse_args()
    
    return {
        'YEAR_MONTH': args.year_month,
        'MINIO_ENDPOINT': args.minio_endpoint,
        'MINIO_ACCESS_KEY': args.minio_access_key,
        'MINIO_SECRET_KEY': args.minio_secret_key,
        'SPARK_MASTER_URL': args.spark_master,
        'INPUT_BUCKET': args.input_bucket,
        'OUTPUT_BUCKET': args.output_bucket
    }


def get_s3a_jars(config):
    """Get S3A JARs - JARs are now configured in the DAG"""
    print("🏗️ JARs are configured in the DAG - no local JAR management needed")
    return []


def create_spark_session(config):
    """Create Spark session with MinIO configuration for transformation workload"""
    # Set Python executable paths for different environments
    import sys
    driver_python = sys.executable  # Python in Airflow container
    worker_python = "/opt/bitnami/python/bin/python3"  # Python in Spark worker container
    
    print(f"🐍 Driver Python executable: {driver_python}")
    print(f"🐍 Worker Python executable: {worker_python}")
    
    # Initialize findspark only for local mode
    if not config['SPARK_MASTER_URL'].startswith('spark://'):
        try:
            findspark.init()
            print("🔧 findspark initialized for local mode")
        except Exception as e:
            print(f"⚠️ findspark init warning: {e}")
    else:
        print("🏗️ Skipping findspark init for cluster mode")
    
    # Get JARs (download for local mode, use cluster JARs for cluster mode)
    jar_paths = get_s3a_jars(config)
    
    # Create Spark session with comprehensive MinIO configuration
    # Note: JARs and basic Spark config are now handled in the DAG
    spark = SparkSession.builder \
        .appName("DivvyBikes_DataTransformation_Airflow") \
        .master(config['SPARK_MASTER_URL']) \
        .config("spark.pyspark.python", worker_python) \
        .config("spark.pyspark.driver.python", driver_python) \
        .config("spark.executorEnv.PYSPARK_PYTHON", worker_python) \
        .config("spark.python.worker.memory", "1g") \
        .config("spark.python.worker.reuse", "true") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{config['MINIO_ENDPOINT']}") \
        .config("spark.hadoop.fs.s3a.access.key", f"{config['MINIO_ACCESS_KEY']}") \
        .config("spark.hadoop.fs.s3a.secret.key", f"{config['MINIO_SECRET_KEY']}") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.region", "us-east-1") \
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "10000") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "10000") \
        .config("spark.hadoop.fs.s3a.attempts.maximum", "3") \
        .config("spark.hadoop.fs.s3a.retry.interval", "1000") \
        .getOrCreate()

    return spark


def read_raw_data(spark, input_path):
    """Read raw parquet data from MinIO raw zone"""
    print(f"📖 Reading raw data from: {input_path}")
    
    try:
        df = spark.read.parquet(input_path)
        print(f"✅ Raw data loaded: {df.count()} rows, {len(df.columns)} columns")
        print("📊 Raw data schema:")
        df.printSchema()
        return df
    except Exception as e:
        print(f"❌ Error reading raw data: {str(e)}")
        return None


def validate_data_quality(df):
    """Perform data quality checks and validation"""
    print("🔍 Performing data quality checks...")
    
    # Check for null values in critical columns
    critical_columns = ["ride_id", "started_at", "ended_at", "rideable_type"]
    
    print("📊 Data Quality Report:")
    print(f"Total records: {df.count()}")
    
    # Check for nulls in critical columns
    for col_name in critical_columns:
        if col_name in df.columns:
            null_count = df.filter(col(col_name).isNull()).count()
            print(f"  - {col_name}: {null_count} null values")
    
    # Check for duplicate ride_ids
    total_rides = df.count()
    unique_rides = df.select("ride_id").distinct().count()
    duplicate_rides = total_rides - unique_rides
    print(f"  - Duplicate ride_ids: {duplicate_rides}")
    
    # Check date range
    date_range = df.select(
        min(col("started_at")).alias("min_date"),
        max(col("started_at")).alias("max_date")
    ).collect()[0]
    print(f"  - Date range: {date_range['min_date']} to {date_range['max_date']}")
    
    # Check for invalid coordinates (outside Chicago area approximately)
    invalid_coords = df.filter(
        (col("start_lat") < 41.0) | (col("start_lat") > 42.5) |
        (col("start_lng") < -88.5) | (col("start_lng") > -87.0) |
        (col("end_lat") < 41.0) | (col("end_lat") > 42.5) |
        (col("end_lng") < -88.5) | (col("end_lng") > -87.0)
    ).count()
    print(f"  - Records with invalid coordinates: {invalid_coords}")
    
    return df


def clean_and_transform_data(df):
    """Clean and transform the data"""
    print("🧹 Cleaning and transforming data...")
    
    # Remove duplicates based on ride_id
    df_clean = df.dropDuplicates(["ride_id"])
    print(f"✅ Removed duplicates: {df.count() - df_clean.count()} records")
    
    # Filter out records with null critical values
    df_clean = df_clean.filter(
        col("ride_id").isNotNull() &
        col("started_at").isNotNull() &
        col("ended_at").isNotNull() &
        col("rideable_type").isNotNull()
    )
    print(f"✅ Filtered null critical values: {df_clean.count()} records remaining")
    
    # Convert timestamp columns to proper timestamp format
    df_clean = df_clean.withColumn(
        "started_at_ts", 
        to_timestamp(col("started_at"), "yyyy-MM-dd HH:mm:ss")
    ).withColumn(
        "ended_at_ts", 
        to_timestamp(col("ended_at"), "yyyy-MM-dd HH:mm:ss")
    )
    
    # Calculate trip duration in minutes
    df_clean = df_clean.withColumn(
        "trip_duration_minutes",
        (col("ended_at_ts").cast("long") - col("started_at_ts").cast("long")) / 60
    )
    
    # Add derived date/time columns
    df_clean = df_clean.withColumn("start_date", to_date(col("started_at_ts"))) \
                       .withColumn("start_hour", hour(col("started_at_ts"))) \
                       .withColumn("start_day_of_week", dayofweek(col("started_at_ts"))) \
                       .withColumn("start_month", month(col("started_at_ts"))) \
                       .withColumn("start_year", year(col("started_at_ts")))
    
    # Calculate trip distance (approximate using Haversine formula)
    df_clean = df_clean.withColumn(
        "trip_distance_km",
        acos(
            sin(radians(col("start_lat"))) * sin(radians(col("end_lat"))) +
            cos(radians(col("start_lat"))) * cos(radians(col("end_lat"))) *
            cos(radians(col("end_lng")) - radians(col("start_lng")))
        ) * 6371  # Earth's radius in km
    )
    
    # Filter out obviously invalid trips (too short or too long)
    df_clean = df_clean.filter(
        (col("trip_duration_minutes") > 1) &  # At least 1 minute
        (col("trip_duration_minutes") < 1440) &  # Less than 24 hours
        (col("trip_distance_km") < 100)  # Less than 100km
    )
    print(f"✅ Filtered invalid trips: {df_clean.count()} records remaining")
    
    # Standardize rideable_type values
    df_clean = df_clean.withColumn(
        "rideable_type_clean",
        when(col("rideable_type").isin(["electric_bike", "classic_bike", "docked_bike"]), 
             col("rideable_type")).otherwise("unknown")
    )
    
    # Standardize member_casual values
    df_clean = df_clean.withColumn(
        "member_casual_clean",
        when(col("member_casual").isin(["member", "casual"]), 
             col("member_casual")).otherwise("unknown")
    )
    
    # Add data processing metadata
    df_clean = df_clean.withColumn("processed_at", current_timestamp()) \
                       .withColumn("processing_date", current_date())
    
    print(f"✅ Data transformation completed: {df_clean.count()} records")
    print("📊 Transformed data schema:")
    df_clean.printSchema()
    
    return df_clean


def add_business_metrics(df):
    """Add business-relevant metrics and categorizations"""
    print("📈 Adding business metrics...")
    
    # Categorize trip duration
    df = df.withColumn(
        "trip_duration_category",
        when(col("trip_duration_minutes") <= 5, "very_short")
        .when(col("trip_duration_minutes") <= 15, "short")
        .when(col("trip_duration_minutes") <= 30, "medium")
        .when(col("trip_duration_minutes") <= 60, "long")
        .otherwise("very_long")
    )
    
    # Categorize trip distance
    df = df.withColumn(
        "trip_distance_category",
        when(col("trip_distance_km") <= 1, "very_short")
        .when(col("trip_distance_km") <= 3, "short")
        .when(col("trip_distance_km") <= 5, "medium")
        .when(col("trip_distance_km") <= 10, "long")
        .otherwise("very_long")
    )
    
    # Time of day categorization
    df = df.withColumn(
        "time_of_day",
        when(col("start_hour").between(6, 11), "morning")
        .when(col("start_hour").between(12, 17), "afternoon")
        .when(col("start_hour").between(18, 21), "evening")
        .otherwise("night")
    )
    
    # Day type categorization
    df = df.withColumn(
        "day_type",
        when(col("start_day_of_week").isin([1, 7]), "weekend")
        .otherwise("weekday")
    )
    
    # Season categorization (Northern Hemisphere)
    df = df.withColumn(
        "season",
        when(col("start_month").isin([12, 1, 2]), "winter")
        .when(col("start_month").isin([3, 4, 5]), "spring")
        .when(col("start_month").isin([6, 7, 8]), "summer")
        .otherwise("fall")
    )
    
    print("✅ Business metrics added successfully")
    return df


def save_transformed_data(df, output_path, partition_columns=None, spark_session=None):
    """Save transformed DataFrame as Parquet to MinIO staging zone"""
    try:
        print(f"💾 Saving transformed data to staging zone...")
        print(f"📍 Path: {output_path}")
        
        # Prepare write operation
        writer = df.write.mode("overwrite") \
                   .option("compression", "snappy") \
                   .option("spark.sql.parquet.compression.codec", "snappy")
        
        # Add partitioning if specified
        if partition_columns:
            writer = writer.partitionBy(*partition_columns)
            print(f"📅 Partitioned by: {', '.join(partition_columns)}")
        
        # Save as Parquet
        writer.parquet(output_path)
        
        print(f"✅ Successfully saved to MinIO staging zone!")
        print(f"📍 Location: {output_path}")
        
        # Verify the save
        verify_df = spark_session.read.parquet(output_path)
        verify_count = verify_df.count()
        original_count = df.count()
        
        print(f"✅ Verification: {original_count} → {verify_count} rows")
        
        # Show partition info if partitioned
        if partition_columns:
            print("📊 Partition information:")
            for col_name in partition_columns:
                verify_df.select(col_name).distinct().orderBy(col_name).show(10, truncate=False)
        
        return True, output_path
        
    except Exception as e:
        print(f"❌ Error saving transformed data: {str(e)}")
        return False, None


def generate_data_summary(df):
    """Generate a summary of the transformed data"""
    print("📋 Generating data summary...")
    
    summary_stats = df.agg(
        count("*").alias("total_records"),
        countDistinct("ride_id").alias("unique_rides"),
        min("start_date").alias("earliest_date"),
        max("start_date").alias("latest_date"),
        avg("trip_duration_minutes").alias("avg_duration_minutes"),
        avg("trip_distance_km").alias("avg_distance_km")
    ).collect()[0]
    
    print("📊 Data Summary:")
    print(f"  - Total records: {summary_stats['total_records']:,}")
    print(f"  - Unique rides: {summary_stats['unique_rides']:,}")
    print(f"  - Date range: {summary_stats['earliest_date']} to {summary_stats['latest_date']}")
    print(f"  - Average trip duration: {summary_stats['avg_duration_minutes']:.2f} minutes")
    print(f"  - Average trip distance: {summary_stats['avg_distance_km']:.2f} km")
    
    # Show distribution by categories
    print("\n📊 Distribution by categories:")
    
    categories = ["rideable_type_clean", "member_casual_clean", "trip_duration_category", 
                  "time_of_day", "day_type", "season"]
    
    for category in categories:
        if category in df.columns:
            print(f"\n{category}:")
            df.groupBy(category).count().orderBy(desc("count")).show(10, truncate=False)


def main():
    """Main execution function"""
    print("🚀 Starting Divvy Bikes Data Transformation Pipeline")
    print("=" * 60)
    
    # Get configuration from command line arguments
    config = get_config_from_args()
    
    year_month = config['YEAR_MONTH']
    
    # Paths
    input_bucket = config['INPUT_BUCKET']
    output_bucket = config['OUTPUT_BUCKET']
    Table_name = "table_banesco_divvy_bikes"
    input_path = f"s3a://{input_bucket}/divvy-bikes/"
    output_path = f"s3a://{output_bucket}/{Table_name}"
    
    print(f"📊 Processing data for: {year_month}")
    print(f"📥 Input bucket: {input_bucket}")
    print(f"💾 Output bucket: {output_bucket}")
    print(f"📍 Input path: {input_path}")
    print(f"📍 Output path: {output_path}")
    
    try:
        # Initialize Spark session
        spark = create_spark_session(config)
        
        # Read raw data
        raw_df = read_raw_data(spark, input_path)
        if raw_df is None:
            print("❌ Failed to read raw data. Exiting...")
            return
        
        # Validate data quality
        validated_df = validate_data_quality(raw_df)
        
        # Clean and transform data
        transformed_df = clean_and_transform_data(validated_df)
        
        # Add business metrics
        final_df = add_business_metrics(transformed_df)
        
        # Generate summary before saving
        generate_data_summary(final_df)
        
        # Save transformed data with partitioning
        partition_columns = ["processing_date"]
        success, saved_path = save_transformed_data(final_df, output_path, partition_columns, spark)
        
        if success:
            print("\n🎉 Data transformation pipeline completed successfully!")
            print(f"📍 Transformed data saved to: {saved_path}")
        else:
            print("\n❌ Data transformation pipeline failed during save operation")
            sys.exit(1)
        
    except Exception as e:
        print(f"\n❌ Pipeline failed with error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        if 'spark' in locals():
            spark.stop()
            print("🛑 Spark session stopped")



if __name__ == "__main__":
    main()
