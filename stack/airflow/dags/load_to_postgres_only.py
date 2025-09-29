from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Default arguments
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False
}

# Define the DAG
dag = DAG(
    'load_to_postgres_only',
    default_args=default_args,
    description='Load Divvy Bikes data from MinIO to PostgreSQL - Standalone',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['data', 'bike', 'postgres', 'load'],
)

# Single task: Load data to PostgreSQL
load_to_postgres_task = SparkSubmitOperator(
    task_id='load_to_postgres',
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
