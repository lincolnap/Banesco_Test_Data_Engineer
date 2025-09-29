"""
DAG for testing the cleanup and load functionality
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable

# Import custom operator for data cleanup
import sys
import os
sys.path.append('/opt/airflow/plugins')
from divvy_bikes_cleanup_operator import DivvyBikesCleanupOperator

# Function to get Airflow variables with fallback values
def get_airflow_var(var_name, default_value=None):
    """Get Airflow variable with fallback to default value"""
    try:
        return Variable.get(var_name)
    except KeyError:
        if default_value is not None:
            return default_value
        raise

# DAG Configuration
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    dag_id='cleanup_and_load_test',
    default_args=default_args,
    description='Test DAG for cleanup and load operations',
    schedule_interval=None,  # Manual trigger
    catchup=False,
    tags=['divvy_bikes', 'test', 'cleanup', 'load'],
) as dag:

    # Task 1: Cleanup PostgreSQL data before loading
    cleanup_postgres_task = DivvyBikesCleanupOperator(
        task_id='Cleanup_PostgreSQL_Data',
        year_month='{{ var.value.YearMon }}',
        postgres_conn_id='postgres_default',
        cleanup_mode='date_range',  # Options: 'date_range', 'truncate_all', 'cascade_delete'
        dag=dag,
    )

    # Task 2: Load data to PostgreSQL
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

    # Define task dependencies
    cleanup_postgres_task >> load_to_postgres_task
