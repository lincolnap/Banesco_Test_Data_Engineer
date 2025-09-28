from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Default arguments
default_args = {
    'owner': 'banesco_data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'banesco_data_pipeline',
    default_args=default_args,
    description='Banesco Data Engineering Pipeline',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['banesco', 'data', 'pipeline'],
)

# Task 1: Extract data
def extract_data():
    """Extract data from various sources"""
    print("Extracting data from source systems...")
    # Simulate data extraction
    import time
    time.sleep(2)
    print("Data extraction completed successfully!")
    return "extracted_data"

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

# Task 2: Transform data
def transform_data():
    """Transform the extracted data"""
    print("Transforming data...")
    # Simulate data transformation
    import time
    time.sleep(3)
    print("Data transformation completed successfully!")
    return "transformed_data"

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

# Task 3: Load data to PostgreSQL
load_to_postgres = PostgresOperator(
    task_id='load_to_postgres',
    postgres_conn_id='postgres_default',
    sql="""
    CREATE TABLE IF NOT EXISTS daily_summary (
        id SERIAL PRIMARY KEY,
        date DATE,
        total_transactions INTEGER,
        total_amount DECIMAL(15,2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    INSERT INTO daily_summary (date, total_transactions, total_amount)
    VALUES (CURRENT_DATE, 1000, 50000.00);
    """,
    dag=dag,
)

# Task 4: Generate report
def generate_report():
    """Generate a summary report"""
    print("Generating daily report...")
    # Simulate report generation
    import time
    time.sleep(1)
    print("Report generated successfully!")
    return "report_generated"

report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    dag=dag,
)

# Task 5: Cleanup
cleanup_task = BashOperator(
    task_id='cleanup_temp_files',
    bash_command='echo "Cleaning up temporary files..." && sleep 1 && echo "Cleanup completed!"',
    dag=dag,
)

# Define task dependencies
extract_task >> transform_task >> load_to_postgres >> report_task >> cleanup_task
