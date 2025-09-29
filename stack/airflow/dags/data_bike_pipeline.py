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
    """Generate SQL cleanup commands for the current YearMon variable(s)"""
    from airflow.models import Variable
    
    try:
        year_mon_input = Variable.get('YearMon', '202304')
        
        # Parse year_month - can be single value or comma-separated list
        if ',' in year_mon_input:
            year_mon_list = [ym.strip() for ym in year_mon_input.split(',')]
        else:
            year_mon_list = [year_mon_input]
        
        # Format the array for PostgreSQL
        postgres_array = "ARRAY['" + "','".join(year_mon_list) + "']"
        
        cleanup_sql = f"""
        -- Cleanup data for months: {', '.join(year_mon_list)}
        
        DO $$
        DECLARE
            year_month text;
            start_date date;
            end_date date;
            deleted_count int;
            total_processed int := 0;
        BEGIN
            RAISE NOTICE 'Starting cleanup for months: {', '.join(year_mon_list)}';
            
            -- Loop through each year-month
            FOREACH year_month IN ARRAY {postgres_array}
            LOOP
                BEGIN
                    -- Validate basic format first
                    IF LENGTH(year_month) != 6 THEN
                        RAISE NOTICE 'Skipping invalid year_month (wrong length): %', year_month;
                        CONTINUE;
                    END IF;
                    
                    -- Calculate dates using a simpler approach
                    start_date := TO_DATE(year_month, 'YYYYMM');
                    end_date := (start_date + INTERVAL '1 month' - INTERVAL '1 day')::date;
                    
                    RAISE NOTICE 'Processing: % (%, to %)', year_month, start_date, end_date;
                    
                    -- Delete operations in transaction per month
                    DELETE FROM analytics.agg_user_behavior WHERE date_key BETWEEN start_date AND end_date;
                    GET DIAGNOSTICS deleted_count = ROW_COUNT;
                    RAISE NOTICE '  - agg_user_behavior: % rows', deleted_count;
                    
                    DELETE FROM analytics.agg_station_metrics WHERE date_key BETWEEN start_date AND end_date;
                    GET DIAGNOSTICS deleted_count = ROW_COUNT;
                    RAISE NOTICE '  - agg_station_metrics: % rows', deleted_count;
                    
                    DELETE FROM analytics.agg_geographical_analysis WHERE date_key BETWEEN start_date AND end_date;
                    GET DIAGNOSTICS deleted_count = ROW_COUNT;
                    RAISE NOTICE '  - agg_geographical_analysis: % rows', deleted_count;
                    
                    DELETE FROM analytics.agg_daily_metrics WHERE date_key BETWEEN start_date AND end_date;
                    GET DIAGNOSTICS deleted_count = ROW_COUNT;
                    RAISE NOTICE '  - agg_daily_metrics: % rows', deleted_count;
                    
                    DELETE FROM analytics.fact_rides WHERE dt BETWEEN start_date AND end_date;
                    GET DIAGNOSTICS deleted_count = ROW_COUNT;
                    RAISE NOTICE '  - fact_rides: % rows', deleted_count;
                    
                    DELETE FROM analytics.dim_time 
                    WHERE time_key BETWEEN start_date AND end_date
                    AND NOT EXISTS (
                        SELECT 1 FROM analytics.fact_rides WHERE dt = dim_time.time_key
                    );
                    GET DIAGNOSTICS deleted_count = ROW_COUNT;
                    RAISE NOTICE '  - dim_time: % rows', deleted_count;
                    
                    total_processed := total_processed + 1;
                    
                EXCEPTION WHEN OTHERS THEN
                    RAISE WARNING 'Failed to process month %: %', year_month, SQLERRM;
                    CONTINUE;
                END;
            END LOOP;
            
            RAISE NOTICE 'Cleanup completed. Successfully processed % out of % months', 
                         total_processed, array_length({postgres_array}, 1);
        END $$;

        -- Update statistics for query optimizer
        ANALYZE analytics.fact_rides;
        ANALYZE analytics.dim_stations;
        ANALYZE analytics.dim_time;
        ANALYZE analytics.agg_user_behavior;
        ANALYZE analytics.agg_station_metrics;
        ANALYZE analytics.agg_geographical_analysis;
        ANALYZE analytics.agg_daily_metrics;
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
    sql=get_cleanup_sql_for_date_range(),
    dag=dag,
)

# Task 4: Load data to PostgreSQL
load_to_postgres_task = SparkSubmitOperator(
    task_id='Load_to_Postgres',
    application='/opt/airflow/scripts/divvy_bikes_load_to_postgres.py',
    conn_id='spark_default',
    packages='org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0',
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
