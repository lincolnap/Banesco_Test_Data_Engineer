#!/usr/bin/env python3
"""
Setup Airflow Variables for Divvy Bikes Pipeline
Creates the necessary variables for the data pipeline
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

def setup_airflow_variables():
    """Setup Airflow variables for the pipeline"""
    
    # Airflow configuration
    airflow_url = "http://localhost:8080"
    airflow_user = "admin"
    airflow_password = "admin"
    
    # Variables to create
    variables = {
        'YearMon': '202304',
        'MINIO_ENDPOINT': 'minio:9000',
        'MINIO_ACCESS_KEY': 'minioadmin',
        'MINIO_SECRET_KEY': 'minioadmin123',
        'SPARK_MASTER_URL': 'spark://spark-master:7077',
        'POSTGRES_HOST': 'postgres',
        'POSTGRES_PORT': '5432',
        'POSTGRES_DB': 'banesco_test',
        'POSTGRES_USER': 'postgres',
        'POSTGRES_PASSWORD': 'postgres123',
        'SCRIPT_BUCKET': 'banesco-pa-data-artifact',
        'OUTPUT_BUCKET': 'banesco-pa-data-raw-zone',
        'DIVVY_BASE_URL': 'https://divvy-tripdata.s3.amazonaws.com'
    }
    
    print("🔧 Setting up Airflow variables...")
    
    # Setup authentication
    auth_string = f"{airflow_user}:{airflow_password}"
    auth_bytes = auth_string.encode('ascii')
    auth_b64 = base64.b64encode(auth_bytes).decode('ascii')
    
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Basic {auth_b64}'
    }
    
    success_count = 0
    
    for var_name, var_value in variables.items():
        try:
            # Check if variable already exists
            print(f"🔍 Checking variable '{var_name}'...")
            check_url = f"{airflow_url}/api/v1/variables/{var_name}"
            check_response = requests.get(check_url, headers=headers)
            
            var_data = {
                "key": var_name,
                "value": var_value
            }
            
            if check_response.status_code == 200:
                print(f"🔄 Variable '{var_name}' exists. Updating...")
                # Update existing variable
                response = requests.patch(check_url, headers=headers, data=json.dumps(var_data))
            else:
                print(f"➕ Creating new variable '{var_name}'...")
                # Create new variable
                create_url = f"{airflow_url}/api/v1/variables"
                response = requests.post(create_url, headers=headers, data=json.dumps(var_data))
            
            if response.status_code in [200, 201]:
                print(f"✅ Variable '{var_name}' configured successfully!")
                success_count += 1
            else:
                print(f"❌ Failed to configure variable '{var_name}'. Status: {response.status_code}")
                print(f"Response: {response.text}")
                
        except Exception as e:
            print(f"❌ Error setting up variable '{var_name}': {str(e)}")
    
    print(f"\n📊 Successfully configured {success_count}/{len(variables)} variables")
    return success_count == len(variables)

def test_variables():
    """Test that variables are properly configured"""
    print("\n🧪 Testing Airflow variables...")
    
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
    
    test_variables = ['YearMon', 'MINIO_ENDPOINT', 'SPARK_MASTER_URL', 'POSTGRES_HOST']
    
    try:
        for var_name in test_variables:
            get_url = f"{airflow_url}/api/v1/variables/{var_name}"
            response = requests.get(get_url, headers=headers)
            
            if response.status_code == 200:
                var_data = response.json()
                print(f"✅ {var_name}: {var_data.get('value', 'N/A')}")
            else:
                print(f"❌ Failed to retrieve {var_name}. Status: {response.status_code}")
                return False
        
        print("✅ All test variables are properly configured!")
        return True
        
    except Exception as e:
        print(f"❌ Error testing variables: {str(e)}")
        return False

if __name__ == "__main__":
    print("🚀 Airflow Variables Setup for Divvy Bikes Pipeline")
    print("==================================================")
    
    # Wait for Airflow to be ready
    if not wait_for_airflow():
        print("❌ Airflow is not available. Exiting...")
        sys.exit(1)
    
    # Setup variables
    success = setup_airflow_variables()
    
    if success:
        # Test the variables
        test_success = test_variables()
        if test_success:
            print("\n🎉 Airflow variables setup completed successfully!")
            sys.exit(0)
        else:
            print("\n⚠️ Variables configured but test failed")
            sys.exit(1)
    else:
        print("\n❌ Airflow variables setup failed!")
        sys.exit(1)