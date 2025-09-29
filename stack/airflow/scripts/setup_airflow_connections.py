"""
Setup Airflow Connections and Variables for Divvy Bikes Pipeline
"""

import os
import sys
from airflow.models import Connection, Variable
from airflow.utils.db import create_session
from airflow.utils.dates import days_ago


def setup_connections():
    """Create necessary connections in Airflow"""
    print("🔧 Setting up Airflow connections...")
    
    with create_session() as session:
        # Check if connections already exist
        existing_conns = session.query(Connection).filter(
            Connection.conn_id.in_(['spark_default', 'minio_default'])
        ).all()
        
        existing_conn_ids = [conn.conn_id for conn in existing_conns]
        
        # Spark connection
        if 'spark_default' not in existing_conn_ids:
            spark_conn = Connection(
                conn_id='spark_default',
                conn_type='spark',
                host='spark-master',
                port=7077,
                extra='{"queue": "default", "deploy-mode": "cluster"}'
            )
            session.add(spark_conn)
            print("✅ Created spark_default connection")
        else:
            print("ℹ️  spark_default connection already exists")
        
        # MinIO connection
        if 'minio_default' not in existing_conn_ids:
            minio_conn = Connection(
                conn_id='minio_default',
                conn_type='s3',
                host='minio',
                port=9000,
                login='minioadmin',
                password='minioadmin123',
                extra='{"aws_access_key_id": "minioadmin", "aws_secret_access_key": "minioadmin123", "endpoint_url": "http://minio:9000"}'
            )
            session.add(minio_conn)
            print("✅ Created minio_default connection")
        else:
            print("ℹ️  minio_default connection already exists")
        
        session.commit()
        print("✅ All connections configured successfully!")


def setup_variables():
    """Create necessary variables in Airflow"""
    print("🔧 Setting up Airflow variables...")
    
    variables_to_create = {
        'MINIO_ENDPOINT': 'minio:9000',
        'MINIO_ACCESS_KEY': 'minioadmin',
        'MINIO_SECRET_KEY': 'minioadmin123',
        'SPARK_MASTER_URL': 'spark://spark-master:7077',
        'SCRIPT_BUCKET': 'banesco-pa-data-artifact',
        'OUTPUT_BUCKET': 'banesco-pa-data-raw-zone',
        'DIVVY_BASE_URL': 'https://divvy-tripdata.s3.amazonaws.com'
    }
    
    for var_name, var_value in variables_to_create.items():
        try:
            # Check if variable exists
            existing_var = Variable.get(var_name, default_var=None)
            if existing_var is None:
                Variable.set(var_name, var_value)
                print(f"✅ Created variable: {var_name}")
            else:
                print(f"ℹ️  Variable {var_name} already exists")
        except Exception as e:
            print(f"❌ Error creating variable {var_name}: {str(e)}")
    
    print("✅ All variables configured successfully!")


def verify_setup():
    """Verify that connections and variables are properly configured"""
    print("🔍 Verifying setup...")
    
    # Verify connections
    with create_session() as session:
        connections = session.query(Connection).filter(
            Connection.conn_id.in_(['spark_default', 'minio_default'])
        ).all()
        
        print(f"📊 Found {len(connections)} connections:")
        for conn in connections:
            print(f"  - {conn.conn_id}: {conn.conn_type}@{conn.host}:{conn.port}")
    
    # Verify variables
    variables_to_check = ['MINIO_ENDPOINT', 'SPARK_MASTER_URL', 'SCRIPT_BUCKET']
    print(f"📊 Checking {len(variables_to_check)} variables:")
    for var_name in variables_to_check:
        try:
            var_value = Variable.get(var_name)
            print(f"  - {var_name}: {var_value}")
        except Exception as e:
            print(f"  - {var_name}: ❌ Error - {str(e)}")
    
    print("✅ Setup verification completed!")


def main():
    """Main setup function"""
    print("🚀 Starting Airflow setup for Divvy Bikes Pipeline...")
    
    try:
        setup_connections()
        setup_variables()
        verify_setup()
        print("🎉 Airflow setup completed successfully!")
        
    except Exception as e:
        print(f"❌ Error during setup: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
