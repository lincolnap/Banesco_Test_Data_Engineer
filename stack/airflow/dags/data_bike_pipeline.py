from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Import for cleanup functionality
from airflow.operators.python import PythonOperator
import subprocess

# Function to get Airflow variables with fallback values
def get_airflow_var(var_name, default_value=None):
    """Get Airflow variable with fallback to default value"""
    try:
        return Variable.get(var_name)
    except KeyError:
        print(f"Warning: Variable {var_name} not found, using default: {default_value}")
        return default_value

def get_cleanup_sql_for_date_range():
    """Generate SQL cleanup commands for the current YearMon variable"""
    from datetime import datetime, timedelta
    
    try:
        year_month = Variable.get('YearMon', '202304')
        year = int(year_month[:4])
        month = int(year_month[4:6])
        
        start_date = datetime(year, month, 1).date()
        if month == 12:
            end_date = datetime(year + 1, 1, 1).date() - timedelta(days=1)
        else:
            end_date = datetime(year, month + 1, 1).date() - timedelta(days=1)
        
        # Generate cleanup SQL
        cleanup_sql = f"""
        -- Cleanup data for {year_month} ({start_date} to {end_date})
        DELETE FROM analytics.agg_user_behavior WHERE date_key BETWEEN '{start_date}' AND '{end_date}';
        DELETE FROM analytics.agg_station_metrics WHERE date_key BETWEEN '{start_date}' AND '{end_date}';
        DELETE FROM analytics.agg_geographical_analysis WHERE date_key BETWEEN '{start_date}' AND '{end_date}';
        DELETE FROM analytics.agg_daily_metrics WHERE date_key BETWEEN '{start_date}' AND '{end_date}';
        DELETE FROM analytics.fact_rides WHERE dt BETWEEN '{start_date}' AND '{end_date}';
        DELETE FROM analytics.dim_time WHERE time_key BETWEEN '{start_date}' AND '{end_date}';

        -- Analyze tables for performance
        ANALYZE analytics.fact_rides;
        ANALYZE analytics.dim_stations;
        ANALYZE analytics.dim_time;
        """
        
        return cleanup_sql.strip()
        
    except Exception as e:
        print(f"Error generating cleanup SQL: {str(e)}")
        raise

# Default arguments
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

# Define the DAG
dag = DAG(
    'data_bike_pipeline',
    default_args=default_args,
    description='Data Bike Pipeline',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['data', 'bike', 'pipeline'],
)

# Task 1: Extract Divvy Bikes data using Spark
extract_task = SparkSubmitOperator(
    task_id='Extract_Data',
    application='/opt/airflow/scripts/divvy_bikes_extract.py',
    conn_id='spark_default',
    packages='org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262',
    jars='/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar',
    conf={
        'spark.sql.adaptive.enabled': 'true',
        'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
        'spark.driver.memory': '2g',
        'spark.executor.memory': '2g',
        'spark.executor.cores': '2',
        'spark.executor.instances': '2',
        'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
        'spark.hadoop.fs.s3a.path.style.access': 'true',
        'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
        'spark.hadoop.fs.s3a.aws.credentials.provider': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider'
    },
    application_args=[
        '--year-month', '{{ var.value.YearMon }}',
        '--minio-endpoint', '{{ var.value.MINIO_ENDPOINT }}',
        '--minio-access-key', '{{ var.value.MINIO_ACCESS_KEY }}',
        '--minio-secret-key', '{{ var.value.MINIO_SECRET_KEY }}',
        '--spark-master', 'spark://spark-master:7077',
        '--output-bucket', 'banesco-pa-data-raw-zone'
    ],
    dag=dag,
)

# Task 2: Transform Divvy Bikes data using Spark
transform_task = SparkSubmitOperator(
    task_id='Transform_Data',
    application='/opt/airflow/scripts/divvy_bikes_transformation.py',
    conn_id='spark_default',
    packages='org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262',
    jars='/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar',
    conf={
        'spark.sql.adaptive.enabled': 'true',
        'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
        'spark.driver.memory': '2g',
        'spark.executor.memory': '2g',
        'spark.executor.cores': '2',
        'spark.executor.instances': '2',
        'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
        'spark.hadoop.fs.s3a.path.style.access': 'true',
        'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
        'spark.hadoop.fs.s3a.aws.credentials.provider': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider'
    },
    application_args=[
        '--year-month', '{{ var.value.YearMon }}',
        '--minio-endpoint', '{{ var.value.MINIO_ENDPOINT }}',
        '--minio-access-key', '{{ var.value.MINIO_ACCESS_KEY }}',
        '--minio-secret-key', '{{ var.value.MINIO_SECRET_KEY }}',
        '--spark-master', 'spark://spark-master:7077',
        '--input-bucket', 'banesco-pa-data-raw-zone',
        '--output-bucket', 'banesco-pa-data-stage-zone'
    ],
    dag=dag,
)

# Task 3: Cleanup PostgreSQL data before loading  
cleanup_postgres_task = PostgresOperator(
    task_id='Cleanup_PostgreSQL_Data',
    postgres_conn_id='postgres_default',
    sql="""
    -- Cleanup data for month {{ var.value.YearMon }}
    
    -- Calculate date range
    DO $$
    DECLARE
        start_date date;
        end_date date;
        deleted_count int;
    BEGIN
        -- Calculate start and end dates for the month
        start_date := DATE_TRUNC('month', TO_DATE('{{ var.value.YearMon }}01', 'YYYYMMDD'))::date;
        end_date := (DATE_TRUNC('month', TO_DATE('{{ var.value.YearMon }}01', 'YYYYMMDD')) + INTERVAL '1 month' - INTERVAL '1 day')::date;
        
        RAISE NOTICE 'Cleaning up data for period: % to %', start_date, end_date;
        
        -- Delete aggregated tables first (no FK dependencies)
        DELETE FROM analytics.agg_user_behavior WHERE date_key BETWEEN start_date AND end_date;
        GET DIAGNOSTICS deleted_count = ROW_COUNT;
        RAISE NOTICE 'Deleted % rows from agg_user_behavior', deleted_count;
        
        DELETE FROM analytics.agg_station_metrics WHERE date_key BETWEEN start_date AND end_date;
        GET DIAGNOSTICS deleted_count = ROW_COUNT;
        RAISE NOTICE 'Deleted % rows from agg_station_metrics', deleted_count;
        
        DELETE FROM analytics.agg_geographical_analysis WHERE date_key BETWEEN start_date AND end_date;
        GET DIAGNOSTICS deleted_count = ROW_COUNT;
        RAISE NOTICE 'Deleted % rows from agg_geographical_analysis', deleted_count;
        
        DELETE FROM analytics.agg_daily_metrics WHERE date_key BETWEEN start_date AND end_date;
        GET DIAGNOSTICS deleted_count = ROW_COUNT;
        RAISE NOTICE 'Deleted % rows from agg_daily_metrics', deleted_count;
        
        -- Delete fact table
        DELETE FROM analytics.fact_rides WHERE dt BETWEEN start_date AND end_date;
        GET DIAGNOSTICS deleted_count = ROW_COUNT;
        RAISE NOTICE 'Deleted % rows from fact_rides', deleted_count;
        
        -- Delete dim_time (only if no other facts reference it)
        DELETE FROM analytics.dim_time WHERE time_key BETWEEN start_date AND end_date
        AND NOT EXISTS (SELECT 1 FROM analytics.fact_rides WHERE dt = dim_time.time_key);
        GET DIAGNOSTICS deleted_count = ROW_COUNT;
        RAISE NOTICE 'Deleted % rows from dim_time', deleted_count;
        
        RAISE NOTICE 'Cleanup completed successfully for {{ var.value.YearMon }}';
    END $$;
    
    -- Analyze tables for performance
    ANALYZE analytics.fact_rides;
    ANALYZE analytics.dim_stations;
    ANALYZE analytics.dim_time;
    """,
    dag=dag,
)

# Task 4: Load data to PostgreSQL
load_to_postgres_task = SparkSubmitOperator(
    task_id='Load_to_Postgres',
    application='/opt/airflow/scripts/divvy_bikes_load_to_postgres.py',
    conn_id='spark_default',
    packages='org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0',
    jars='/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar',
    conf={
        'spark.sql.adaptive.enabled': 'true',
        'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
        'spark.driver.memory': '2g',
        'spark.executor.memory': '2g',
        'spark.executor.cores': '2',
        'spark.executor.instances': '2',
        'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
        'spark.hadoop.fs.s3a.path.style.access': 'true',
        'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
        'spark.hadoop.fs.s3a.aws.credentials.provider': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider'
    },
    application_args=[
        '--year-month', '{{ var.value.YearMon }}',
        '--minio-endpoint', '{{ var.value.MINIO_ENDPOINT }}',
        '--minio-access-key', '{{ var.value.MINIO_ACCESS_KEY }}',
        '--minio-secret-key', '{{ var.value.MINIO_SECRET_KEY }}',
        '--postgres-host', '{{ var.value.POSTGRES_HOST }}',
        '--postgres-port', '{{ var.value.POSTGRES_PORT }}',
        '--postgres-db', '{{ var.value.POSTGRES_DB }}',
        '--postgres-user', '{{ var.value.POSTGRES_USER }}',
        '--postgres-password', '{{ var.value.POSTGRES_PASSWORD }}',
        '--input-bucket', 'banesco-pa-data-stage-zone',
        '--spark-master', '{{ var.value.SPARK_MASTER_URL }}'
    ],
    dag=dag,
)


extract_task >> transform_task >> cleanup_postgres_task >> load_to_postgres_task
