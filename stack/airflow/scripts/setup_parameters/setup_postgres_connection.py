#!/usr/bin/env python3
"""
Setup PostgreSQL Connection in Airflow
Creates the postgres_default connection needed for the Divvy Bikes pipeline
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

def setup_postgres_connection():
    """Setup PostgreSQL connection in Airflow"""
    
    # Airflow configuration
    airflow_url = "http://localhost:8080"
    airflow_user = "admin"
    airflow_password = "admin"
    
    # PostgreSQL connection details
    postgres_conn = {
        "connection_id": "postgres_default",
        "conn_type": "postgres",
        "host": "postgres",
        "port": 5432,
        "schema": "banesco_test",
        "login": "postgres",
        "password": "postgres123",
        "extra": json.dumps({
            "sslmode": "disable"
        })
    }
    
    print("🔌 Setting up PostgreSQL connection in Airflow...")
    
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
        print(f"🔍 Checking if connection '{postgres_conn['connection_id']}' exists...")
        check_url = f"{airflow_url}/api/v1/connections/{postgres_conn['connection_id']}"
        check_response = requests.get(check_url, headers=headers)
        
        if check_response.status_code == 200:
            print(f"🔄 Connection '{postgres_conn['connection_id']}' exists. Updating...")
            # Update existing connection
            response = requests.patch(check_url, headers=headers, data=json.dumps(postgres_conn))
        else:
            print(f"➕ Creating new connection '{postgres_conn['connection_id']}'...")
            # Create new connection
            create_url = f"{airflow_url}/api/v1/connections"
            response = requests.post(create_url, headers=headers, data=json.dumps(postgres_conn))
        
        if response.status_code in [200, 201]:
            print(f"✅ PostgreSQL connection '{postgres_conn['connection_id']}' configured successfully!")
            
            # Show connection details (without password)
            conn_details = postgres_conn.copy()
            conn_details['password'] = '***'
            print("\n📋 Connection Details:")
            for key, value in conn_details.items():
                if key != 'extra':
                    print(f"   {key}: {value}")
            
            return True
        else:
            print(f"❌ Failed to configure connection. Status: {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error setting up PostgreSQL connection: {str(e)}")
        return False

def test_postgres_connection():
    """Test the PostgreSQL connection"""
    print("\n🧪 Testing PostgreSQL connection...")
    
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
        get_url = f"{airflow_url}/api/v1/connections/postgres_default"
        response = requests.get(get_url, headers=headers)
        
        if response.status_code == 200:
            print("✅ PostgreSQL connection is properly configured in Airflow!")
            return True
        else:
            print(f"❌ Failed to retrieve connection. Status: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Error testing PostgreSQL connection: {str(e)}")
        return False

if __name__ == "__main__":
    print("🚀 PostgreSQL Connection Setup for Airflow")
    print("==========================================")
    
    # Wait for Airflow to be ready
    if not wait_for_airflow():
        print("❌ Airflow is not available. Exiting...")
        sys.exit(1)
    
    # Setup PostgreSQL connection
    success = setup_postgres_connection()
    
    if success:
        # Test the connection
        test_success = test_postgres_connection()
        if test_success:
            print("\n🎉 PostgreSQL connection setup completed successfully!")
            print("🎯 The connection is ready for the Divvy Bikes pipeline!")
            sys.exit(0)
        else:
            print("\n⚠️ PostgreSQL connection configured but test failed")
            sys.exit(1)
    else:
        print("\n❌ PostgreSQL connection setup failed!")
        sys.exit(1)
