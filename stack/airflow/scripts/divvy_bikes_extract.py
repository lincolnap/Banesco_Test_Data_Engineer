"""
Divvy Bikes Data Extraction - Airflow Compatible
Download CSV from ZIP → Read with Spark → Save as Parquet to MinIO
"""

import os
import sys
import argparse
import findspark
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import requests
import zipfile
import io
import pandas as pd


def get_config_from_args():
    """Get configuration from command line arguments (passed by Airflow)"""
    parser = argparse.ArgumentParser(description='Divvy Bikes Data Extraction')
    parser.add_argument('--year-month', required=True, help='Year and month in YYYYMM format')
    parser.add_argument('--minio-endpoint', default='minio:9000', help='MinIO endpoint')
    parser.add_argument('--minio-access-key', default='minioadmin', help='MinIO access key')
    parser.add_argument('--minio-secret-key', default='minioadmin123', help='MinIO secret key')
    parser.add_argument('--spark-master', default='spark://spark-master:7077', help='Spark master URL')
    parser.add_argument('--output-bucket', default='banesco-pa-data-raw-zone', help='Output bucket name')
    
    args = parser.parse_args()
    
    return {
        'YEAR_MONTH': args.year_month,
        'MINIO_ENDPOINT': args.minio_endpoint,
        'MINIO_ACCESS_KEY': args.minio_access_key,
        'MINIO_SECRET_KEY': args.minio_secret_key,
        'SPARK_MASTER_URL': args.spark_master,
        'OUTPUT_BUCKET': args.output_bucket
    }


def get_s3a_jars(config):
    """Get S3A JARs - JARs are now configured in the DAG"""
    print("🏗️ JARs are configured in the DAG - no local JAR management needed")
    return []


def create_spark_session(config):
    """Create Spark session with MinIO configuration"""
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
    
    # Create Spark session with MinIO configuration
    # Note: JARs and basic Spark config are now handled in the DAG
    spark = SparkSession.builder \
        .appName("DivvyBikes_DataExtraction_Airflow") \
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


def download_and_extract_csv_with_pandas(zip_url, target_filename):
    """Download ZIP and read CSV - ULTRA SIMPLIFIED"""
    print(f"📥 Downloading: {zip_url}")
    
    with zipfile.ZipFile(io.BytesIO(requests.get(zip_url).content)) as z:
        csv_file = next((f for f in z.namelist() if f.endswith('.csv') and target_filename in f), None)
        if not csv_file: 
            raise ValueError(f"CSV '{target_filename}' not found")
        
        df = pd.read_csv(z.open(csv_file))
        print(f"✅ DataFrame: {len(df)} rows, {len(df.columns)} columns")
        return df


def pandas_to_spark(pandas_df, spark_session):
    """Convert pandas DataFrame to Spark DataFrame"""
    print("🔄 Converting pandas DataFrame to Spark DataFrame...")
    
    # Convert pandas DataFrame to Spark DataFrame
    df_spark = spark_session.createDataFrame(pandas_df)

    df = df_spark.withColumn("dt", to_date(col("started_at"), "yyyy-MM-dd HH:mm:ss"))
    
    print(f"✅ Spark DataFrame created: {df.count()} rows, {len(df.columns)} columns")
    df.printSchema()
   
    return df


def save_parquet(df, partition, output_path, file_name, spark):
    """Save DataFrame as Parquet to MinIO bucket with date partitioning"""
    try:
        print(f"💾 Saving to MinIO bucket with date partitioning...")
        
        # Create full path
        full_path = f"{output_path}{file_name.replace('.csv', '')}"
        print(f"📍 Path: {full_path}")
        
        # Save as Parquet with date partitioning
        df.write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .option("spark.sql.parquet.compression.codec", "snappy") \
            .partitionBy(partition) \
            .parquet(full_path)
        
        print(f"✅ Successfully saved to MinIO with date partitioning!")
        print(f"📍 Location: {full_path}")
        print(f"📅 Partitioned by: dt (date column)")
        
        # Verify
        verify_df = spark.read.parquet(full_path)
        verify_count = verify_df.count()
        original_count = df.count()
        
        print(f"✅ Verification: {original_count} → {verify_count} rows")
        
        # Show partition info
        print("📊 Partition information:")
        verify_df.select("dt").distinct().orderBy("dt").show(10, truncate=False)
        
        return True, full_path
        
    except Exception as e:
        print(f"❌ Error saving to MinIO: {str(e)}")
        return False, None


def main():
    """Main execution function"""
    print("🚀 Starting Divvy Bikes Data Extraction...")
    
    # Get configuration from command line arguments
    config = get_config_from_args()
    
    year_mon = config['YEAR_MONTH']
    
    # Source configuration
    zip_url = f"https://divvy-tripdata.s3.amazonaws.com/{year_mon}-divvy-tripdata.zip"
    bucket_name = config['OUTPUT_BUCKET']
    file_name = f"{year_mon}-divvy-tripdata.csv"
    output_path = f"s3a://{bucket_name}/divvy-bikes/"
    
    print(f"📊 Processing data for: {year_mon}")
    print(f"📥 Source URL: {zip_url}")
    print(f"💾 Output bucket: {bucket_name}")
    
    try:
        # Create Spark session
        spark = create_spark_session(config)
        
        # Download and extract CSV
        df_pandas = download_and_extract_csv_with_pandas(zip_url, file_name)
        
        # Convert to Spark DataFrame
        df = pandas_to_spark(df_pandas, spark)
        
        # Save as Parquet
        success, path = save_parquet(df, "dt", output_path, file_name, spark)
        
        if success:
            print(f"✅ Data extraction completed successfully!")
            print(f"📍 Data saved to: {path}")
        else:
            print(f"❌ Data extraction failed!")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Error during execution: {str(e)}")
        sys.exit(1)
    finally:
        # Clean up Spark session
        if 'spark' in locals():
            spark.stop()
            print("🧹 Spark session stopped")


if __name__ == "__main__":
    main()
