from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Function to get Airflow variables with fallback values
def get_airflow_var(var_name, default_value=None):
    """Get Airflow variable with fallback to default value"""
    try:
        return Variable.get(var_name)
    except KeyError:
        print(f"Warning: Variable {var_name} not found, using default: {default_value}")
        return default_value

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
    task_id='extract',
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
    task_id='transform_data',
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
#else:
#    # Fallback to BashOperator if SparkSubmitOperator is not available
#    extract_task = BashOperator(
#        task_id='extract',
#        bash_command='''
#        echo "SparkSubmitOperator not available, using fallback method"
#        echo "This is a placeholder for the extract task"
#        echo "Year-Month: {{ ds_nodash[:6] }}"
#        echo "To fix this, ensure apache-airflow-providers-apache-spark is installed"
#        ''',
#        dag=dag,
#    )
#
## Task 2: Transform data
#def transform_data():
#def transform_data():
#def transform_data():
#    """Transform the extracted data"""
#    print("Transforming data...")
#    
#    # Get configuration from Airflow variables
#    minio_endpoint = get_airflow_var('MINIO_ENDPOINT', 'minio:9000')
#    postgres_host = get_airflow_var('POSTGRES_HOST', 'postgres')
#    postgres_port = get_airflow_var('POSTGRES_PORT', '5432')
#    postgres_db = get_airflow_var('POSTGRES_DB', 'banesco_test')
#    
#    print(f"Configuration loaded:")
#    print(f"  MinIO Endpoint: {minio_endpoint}")
#    print(f"  PostgreSQL Host: {postgres_host}:{postgres_port}")
#    print(f"  PostgreSQL Database: {postgres_db}")
#    
#    # Simulate data transformation
#    import time
#    time.sleep(3)
#    print("Data transformation completed successfully!")
#    return "transformed_data"
#
#transform_task = PythonOperator(
#    task_id='transform_data',
#    python_callable=transform_data,
#    dag=dag,
#)
#
## Task 3: Load data to PostgreSQL
#load_to_postgres = PostgresOperator(
#    task_id='load_to_postgres',
#    postgres_conn_id='postgres_default',
#    sql="""
#    CREATE TABLE IF NOT EXISTS daily_summary (
#        id SERIAL PRIMARY KEY,
#        date DATE,
#        total_transactions INTEGER,
#        total_amount DECIMAL(15,2),
#        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
#    );
#    
#    INSERT INTO daily_summary (date, total_transactions, total_amount)
#    VALUES (CURRENT_DATE, 1000, 50000.00);
#    """,
#    dag=dag,
#)
#
## Task 4: Generate report
#def generate_report():
#    """Generate a summary report"""
#    print("Generating daily report...")
#    
#    # Get configuration from Airflow variables
#    mongo_host = get_airflow_var('MONGO_HOST', 'mongodb')
#    mongo_port = get_airflow_var('MONGO_PORT', '27017')
#    mongo_db = get_airflow_var('MONGO_DB', 'banesco_test')
#    kafka_servers = get_airflow_var('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
#    
#    print(f"Report configuration:")
#    print(f"  MongoDB: {mongo_host}:{mongo_port}/{mongo_db}")
#    print(f"  Kafka: {kafka_servers}")
#    
#    # Simulate report generation
#    import time
#    time.sleep(1)
#    print("Report generated successfully!")
#    return "report_generated"
#
#report_task = PythonOperator(
#    task_id='generate_report',
#    python_callable=generate_report,
#    dag=dag,
#)
#
## Task 5: Cleanup
#cleanup_task = BashOperator(
#    task_id='cleanup_temp_files',
#    bash_command='echo "Cleaning up temporary files..." && sleep 1 && echo "Cleanup completed!"',
#    dag=dag,
#)
#
# Define task dependencies
extract_task >> transform_task
