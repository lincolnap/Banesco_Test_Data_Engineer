#!/usr/bin/env python3
"""
Complete Airflow Setup Script
Sets up Airflow variables and connections directly via database
"""

import sys
import time
import os

def setup_variables():
    """Setup Airflow variables directly via database"""
    print("🔧 Setting up Airflow Variables...")
    
    try:
        from airflow.models import Variable
        from airflow.utils.db import create_session
        
        variables = {
            "YearMon": "202304",
            "MINIO_ENDPOINT": "minio:9000",
            "MINIO_ACCESS_KEY": "minioadmin",
            "MINIO_SECRET_KEY": "minioadmin123",
            "SPARK_MASTER_URL": "spark://spark-master:7077",
            "POSTGRES_HOST": "postgres",
            "POSTGRES_PORT": "5432",
            "POSTGRES_DB": "banesco_test",
            "POSTGRES_USER": "postgres",
            "POSTGRES_PASSWORD": "postgres123",
            "SCRIPT_BUCKET": "banesco-pa-data-artifact",
            "OUTPUT_BUCKET": "banesco-pa-data-raw-zone",
            "DIVVY_BASE_URL": "https://divvy-tripdata.s3.amazonaws.com"
        }
        
        with create_session() as session:
            for key, value in variables.items():
                # Check if variable exists
                existing = session.query(Variable).filter(Variable.key == key).first()
                if existing:
                    existing.set_val(value)
                    print(f"  ✅ Updated variable: {key}")
                else:
                    new_var = Variable(key=key, val=value)
                    session.add(new_var)
                    print(f"  ✅ Created variable: {key}")
            session.commit()
        print("✅ All variables set successfully!")
        return True
    except Exception as e:
        print(f"❌ Error setting variables: {str(e)}")
        return False

def setup_connections():
    """Setup Airflow connections directly via database"""
    print("🔧 Setting up Airflow Connections...")
    
    try:
        from airflow.models import Connection
        from airflow.utils.db import create_session
        
        connections = [
            {
                "conn_id": "postgres_default",
                "conn_type": "postgres",
                "host": "postgres",
                "port": 5432,
                "schema": "banesco_test",
                "login": "postgres",
                "password": "postgres123"
            },
            {
                "conn_id": "spark_default",
                "conn_type": "spark",
                "host": "spark-master",
                "port": 7077,
                "extra": '{"queue": "default", "deploy-mode": "client", "spark-master": "spark://spark-master:7077"}'
            }
        ]
        
        with create_session() as session:
            for conn_data in connections:
                # Check if connection exists
                existing = session.query(Connection).filter(Connection.conn_id == conn_data["conn_id"]).first()
                if existing:
                    # Update existing connection
                    for key, value in conn_data.items():
                        setattr(existing, key, value)
                    print(f"  ✅ Updated connection: {conn_data['conn_id']}")
                else:
                    # Create new connection
                    new_conn = Connection(**conn_data)
                    session.add(new_conn)
                    print(f"  ✅ Created connection: {conn_data['conn_id']}")
            session.commit()
        print("✅ All connections set successfully!")
        return True
    except Exception as e:
        print(f"❌ Error setting connections: {str(e)}")
        return False

def main():
    """Main setup function"""
    print("🎯 Complete Airflow Setup for Divvy Bikes Pipeline")
    print("=" * 60)
    
    # Wait a bit for Airflow database to be ready
    print("⏳ Waiting for Airflow database to be ready...")
    time.sleep(10)
    
    success = True
    
    # Setup variables
    print("\n🚀 Setting up Airflow Variables")
    print("=" * 50)
    if not setup_variables():
        success = False
    
    # Setup connections
    print("\n🚀 Setting up Airflow Connections")
    print("=" * 50)
    if not setup_connections():
        success = False
    
    # Summary
    print(f"\n📊 Setup Summary")
    print("=" * 30)
    
    if success:
        print("✅ All configurations completed successfully!")
        print("🎉 Airflow is ready for the Divvy Bikes pipeline!")
        print("🚀 All services are ready!")
    else:
        print("⚠️ Setup completed with some errors")
        print("🔧 Check the logs above for details")
    
    return 0 if success else 1

if __name__ == "__main__":
    main()
