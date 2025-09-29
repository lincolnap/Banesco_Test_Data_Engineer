#!/usr/bin/env python3
"""
Setup Spark connection for Airflow
This script creates the necessary Spark connection configuration
"""

import os
import sys
from airflow.models import Connection
from airflow.utils.db import create_session

def setup_spark_connection():
    """Setup Spark connection for Airflow"""
    
    # Spark connection configuration
    spark_conn_config = {
        'conn_id': 'spark_default',
        'conn_type': 'spark',
        'host': 'spark-master',
        'port': 7077,
        'extra': {
            'queue': 'root.default',
            'deploy-mode': 'cluster',
            'spark-home': '/opt/bitnami/spark',
            'spark-binary': 'spark-submit',
            'namespace': 'default'
        }
    }
    
    try:
        with create_session() as session:
            # Check if connection already exists
            existing_conn = session.query(Connection).filter(
                Connection.conn_id == spark_conn_config['conn_id']
            ).first()
            
            if existing_conn:
                print(f"✓ Connection '{spark_conn_config['conn_id']}' already exists")
                print(f"  Host: {existing_conn.host}:{existing_conn.port}")
                return True
            
            # Create new connection
            new_conn = Connection(
                conn_id=spark_conn_config['conn_id'],
                conn_type=spark_conn_config['conn_type'],
                host=spark_conn_config['host'],
                port=spark_conn_config['port'],
                extra=str(spark_conn_config['extra'])
            )
            
            session.add(new_conn)
            session.commit()
            
            print(f"✓ Successfully created Spark connection: {spark_conn_config['conn_id']}")
            print(f"  Host: {spark_conn_config['host']}:{spark_conn_config['port']}")
            print(f"  Type: {spark_conn_config['conn_type']}")
            
            return True
            
    except Exception as e:
        print(f"✗ Error creating Spark connection: {e}")
        return False

def verify_spark_connection():
    """Verify Spark connection is working"""
    try:
        from airflow.models import Connection
        from airflow.utils.db import create_session
        
        with create_session() as session:
            conn = session.query(Connection).filter(
                Connection.conn_id == 'spark_default'
            ).first()
            
            if conn:
                print(f"✓ Spark connection verified:")
                print(f"  ID: {conn.conn_id}")
                print(f"  Type: {conn.conn_type}")
                print(f"  Host: {conn.host}:{conn.port}")
                return True
            else:
                print("✗ Spark connection not found")
                return False
                
    except Exception as e:
        print(f"✗ Error verifying Spark connection: {e}")
        return False

if __name__ == "__main__":
    print("=== Setting up Spark Connection for Airflow ===")
    
    # Setup connection
    if setup_spark_connection():
        print("\n=== Verifying Connection ===")
        verify_spark_connection()
    else:
        print("Failed to setup Spark connection")
        sys.exit(1)
    
    print("\n=== Setup Complete ===")
