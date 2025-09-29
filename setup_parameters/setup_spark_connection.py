#!/usr/bin/env python3
"""
Setup Spark Connection in Airflow
Creates the spark_default connection for standalone Spark cluster
"""

import requests
import json
import time
import base64
import sys

def wait_for_airflow(max_retries=30, delay=2):
    """Wait for Airflow webserver to be ready"""
    airflow_url = "http://localhost:8080"
    
    print("⏳ Waiting for Airflow webserver to be ready...")
    
    for attempt in range(max_retries):
        try:
            response = requests.get(f"{airflow_url}/health", timeout=5)
            if response.status_code == 200:
                print("✅ Airflow webserver is ready!")
                return True
        except requests.exceptions.RequestException:
            pass
        
        if attempt < max_retries - 1:
            print(f"⏳ Attempt {attempt + 1}/{max_retries} - Airflow not ready yet, waiting {delay}s...")
            time.sleep(delay)
        else:
            print(f"❌ Airflow not ready after {max_retries} attempts")
            return False
    
    return False

def setup_spark_connection():
    """Setup Spark connection in Airflow for standalone cluster"""
    
    # Airflow configuration
    airflow_url = "http://localhost:8080"
    airflow_user = "admin"
    airflow_password = "admin"
    
    # Spark connection details for standalone cluster
    spark_conn = {
        "connection_id": "spark_default",
        "conn_type": "spark",
        "host": "spark-master",
        "port": 7077,
        "extra": json.dumps({
            "queue": "default",
            "deploy-mode": "client",
            "spark-master": "spark://spark-master:7077"
        })
    }
    
    print("⚡ Setting up Spark connection in Airflow...")
    
    # Setup authentication
    auth_string = f"{airflow_user}:{airflow_password}"
    auth_bytes = auth_string.encode('ascii')
    auth_b64 = base64.b64encode(auth_bytes).decode('ascii')
    
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Basic {auth_b64}'
    }
    
    try:
        # Check if connection already exists
        print(f"🔍 Checking if connection '{spark_conn['connection_id']}' exists...")
        check_url = f"{airflow_url}/api/v1/connections/{spark_conn['connection_id']}"
        check_response = requests.get(check_url, headers=headers)
        
        if check_response.status_code == 200:
            print(f"🔄 Connection '{spark_conn['connection_id']}' exists. Updating...")
            # Update existing connection
            response = requests.patch(check_url, headers=headers, data=json.dumps(spark_conn))
        else:
            print(f"➕ Creating new connection '{spark_conn['connection_id']}'...")
            # Create new connection
            create_url = f"{airflow_url}/api/v1/connections"
            response = requests.post(create_url, headers=headers, data=json.dumps(spark_conn))
        
        if response.status_code in [200, 201]:
            print(f"✅ Spark connection '{spark_conn['connection_id']}' configured successfully!")
            
            # Show connection details
            conn_details = spark_conn.copy()
            print("\n📋 Connection Details:")
            print(f"   Connection ID: {conn_details['connection_id']}")
            print(f"   Type: {conn_details['conn_type']}")
            print(f"   Host: {conn_details['host']}")
            print(f"   Port: {conn_details['port']}")
            print(f"   Mode: Standalone (NOT yarn)")
            
            return True
        else:
            print(f"❌ Failed to configure connection. Status: {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error setting up Spark connection: {str(e)}")
        return False

def test_spark_connection():
    """Test the Spark connection"""
    print("\n🧪 Testing Spark connection...")
    
    airflow_url = "http://localhost:8080"
    airflow_user = "admin"
    airflow_password = "admin"
    
    auth_string = f"{airflow_user}:{airflow_password}"
    auth_bytes = auth_string.encode('ascii')
    auth_b64 = base64.b64encode(auth_bytes).decode('ascii')
    
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Basic {auth_b64}'
    }
    
    try:
        # Get connection details to verify it exists
        get_url = f"{airflow_url}/api/v1/connections/spark_default"
        response = requests.get(get_url, headers=headers)
        
        if response.status_code == 200:
            print("✅ Spark connection is properly configured in Airflow!")
            return True
        else:
            print(f"❌ Failed to retrieve connection. Status: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Error testing Spark connection: {str(e)}")
        return False

if __name__ == "__main__":
    print("🚀 Spark Connection Setup for Airflow")
    print("=====================================")
    
    # Wait for Airflow to be ready
    if not wait_for_airflow():
        print("❌ Airflow is not available. Exiting...")
        sys.exit(1)
    
    # Setup Spark connection
    success = setup_spark_connection()
    
    if success:
        # Test the connection
        test_success = test_spark_connection()
        if test_success:
            print("\n🎉 Spark connection setup completed successfully!")
            print("⚡ Spark jobs will use standalone cluster mode!")
            sys.exit(0)
        else:
            print("\n⚠️ Spark connection configured but test failed")
            sys.exit(1)
    else:
        print("\n❌ Spark connection setup failed!")
        sys.exit(1)
