#!/usr/bin/env python3
"""
Setup PostgreSQL Connection in Airflow
"""

import requests
import json
import time
import base64

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
    
    # Wait for Airflow to be ready
    print("⏳ Waiting for Airflow webserver to be ready...")
    max_retries = 30
    for i in range(max_retries):
        try:
            response = requests.get(f"{airflow_url}/health")
            if response.status_code == 200:
                print("✅ Airflow webserver is ready!")
                break
        except requests.exceptions.RequestException:
            pass
        
        if i == max_retries - 1:
            print("❌ Airflow webserver not ready after 30 attempts")
            return False
        
        print(f"⏳ Attempt {i+1}/{max_retries} - waiting for Airflow...")
        time.sleep(2)
    
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
            return True
        else:
            print(f"❌ Failed to configure connection. Status: {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error setting up PostgreSQL connection: {str(e)}")
        return False

def test_postgres_connection():
    """Test PostgreSQL connection"""
    print("🧪 Testing PostgreSQL connection...")
    
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
        test_url = f"{airflow_url}/api/v1/connections/postgres_default/test"
        response = requests.post(test_url, headers=headers)
        
        if response.status_code == 200:
            result = response.json()
            if result.get('status') == 'success':
                print("✅ PostgreSQL connection test successful!")
                return True
            else:
                print(f"❌ PostgreSQL connection test failed: {result}")
                return False
        else:
            print(f"❌ Failed to test connection. Status: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Error testing PostgreSQL connection: {str(e)}")
        return False

if __name__ == "__main__":
    print("🚀 Starting PostgreSQL connection setup...")
    
    success = setup_postgres_connection()
    if success:
        print("✅ PostgreSQL connection setup completed!")
        
        # Test the connection
        test_success = test_postgres_connection()
        if test_success:
            print("🎉 PostgreSQL connection is working properly!")
        else:
            print("⚠️ PostgreSQL connection configured but test failed")
    else:
        print("❌ PostgreSQL connection setup failed!")
        exit(1)
