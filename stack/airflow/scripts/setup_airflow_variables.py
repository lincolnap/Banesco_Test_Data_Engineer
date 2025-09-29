#!/usr/bin/env python3
"""
Script to set up Airflow variables for the Banesco Data Stack
This script configures all the necessary variables in Airflow UI
"""

import os
import sys
from airflow.models import Variable
from airflow import settings

def setup_airflow_variables():
    """Set up all Airflow variables for the Banesco Data Stack"""
    
    # Define all variables with their default values
    variables = {
        # Spark Configuration
        'SPARK_MASTER_URL': 'spark://spark-master:7077',
        
        # MinIO Configuration
        'MINIO_ENDPOINT': 'minio:9000',
        'MINIO_ACCESS_KEY': 'minioadmin',
        'MINIO_SECRET_KEY': 'minioadmin123',
        'MINIO_SECURE': 'false',
        
        # PostgreSQL Configuration
        'POSTGRES_HOST': 'postgres',
        'POSTGRES_PORT': '5432',
        'POSTGRES_DB': 'banesco_test',
        'POSTGRES_USER': 'postgres',
        'POSTGRES_PASSWORD': 'postgres123',
        
        # MongoDB Configuration
        'MONGO_HOST': 'mongodb',
        'MONGO_PORT': '27017',
        'MONGO_DB': 'banesco_test',
        'MONGO_USER': 'admin',
        'MONGO_PASSWORD': 'admin123',
        
        # Kafka Configuration
        'KAFKA_BOOTSTRAP_SERVERS': 'kafka:9092',
    }
    
    print("🔧 Setting up Airflow Variables for Banesco Data Stack")
    print("=" * 60)
    
    # Set up database session
    session = settings.Session()
    
    try:
        for var_name, var_value in variables.items():
            try:
                # Check if variable already exists
                existing_var = session.query(Variable).filter(Variable.key == var_name).first()
                
                if existing_var:
                    # Update existing variable
                    existing_var.set_val(var_value)
                    print(f"✅ Updated: {var_name} = {var_value}")
                else:
                    # Create new variable
                    new_var = Variable(key=var_name, val=var_value)
                    session.add(new_var)
                    print(f"✅ Created: {var_name} = {var_value}")
                
                session.commit()
                
            except Exception as e:
                print(f"❌ Error setting {var_name}: {str(e)}")
                session.rollback()
        
        print("=" * 60)
        print("🎉 Airflow Variables setup completed successfully!")
        print("\n📋 Variables configured:")
        for var_name in variables.keys():
            print(f"  - {var_name}")
            
    except Exception as e:
        print(f"❌ Error during setup: {str(e)}")
        session.rollback()
        return False
    
    finally:
        session.close()
    
    return True

def verify_variables():
    """Verify that all variables are properly set"""
    print("\n🔍 Verifying Airflow Variables...")
    print("=" * 40)
    
    required_vars = [
        'SPARK_MASTER_URL', 'MINIO_ENDPOINT', 'MINIO_ACCESS_KEY', 'MINIO_SECRET_KEY',
        'POSTGRES_HOST', 'POSTGRES_PORT', 'POSTGRES_DB', 'POSTGRES_USER', 'POSTGRES_PASSWORD',
        'MONGO_HOST', 'MONGO_PORT', 'MONGO_DB', 'MONGO_USER', 'MONGO_PASSWORD',
        'KAFKA_BOOTSTRAP_SERVERS'
    ]
    
    session = settings.Session()
    missing_vars = []
    
    try:
        for var_name in required_vars:
            var = session.query(Variable).filter(Variable.key == var_name).first()
            if var:
                print(f"✅ {var_name}: {var.val}")
            else:
                print(f"❌ {var_name}: NOT FOUND")
                missing_vars.append(var_name)
        
        if missing_vars:
            print(f"\n⚠️  Missing variables: {', '.join(missing_vars)}")
            return False
        else:
            print("\n🎉 All variables are properly configured!")
            return True
            
    except Exception as e:
        print(f"❌ Error during verification: {str(e)}")
        return False
    
    finally:
        session.close()

if __name__ == "__main__":
    print("Banesco Data Stack - Airflow Variables Setup")
    print("=" * 50)
    
    # Set up variables
    if setup_airflow_variables():
        # Verify variables
        if verify_variables():
            print("\n✅ Setup completed successfully!")
            sys.exit(0)
        else:
            print("\n❌ Setup completed with errors!")
            sys.exit(1)
    else:
        print("\n❌ Setup failed!")
        sys.exit(1)
