#!/usr/bin/env python3
"""
Test script for Divvy Bikes Data Pipeline DAG
This script helps test the DAG before running it in Airflow
"""

import os
import sys
import subprocess
import time
from datetime import datetime

def check_docker_services():
    """Check if required Docker services are running"""
    print("🔍 Checking Docker services...")
    
    services = {
        'postgres': '5433',
        'minio': '9000',
        'spark-master': '8081',
        'airflow-webserver': '8080'
    }
    
    all_running = True
    
    for service, port in services.items():
        try:
            result = subprocess.run(
                ['docker', 'ps', '--filter', f'name={service}', '--format', 'table {{.Names}}\t{{.Status}}'],
                capture_output=True, text=True, timeout=10
            )
            
            if service in result.stdout and 'Up' in result.stdout:
                print(f"✅ {service}: Running on port {port}")
            else:
                print(f"❌ {service}: Not running")
                all_running = False
                
        except Exception as e:
            print(f"❌ {service}: Error checking status - {str(e)}")
            all_running = False
    
    return all_running

def test_postgres_connection():
    """Test PostgreSQL connection"""
    print("\n🔍 Testing PostgreSQL connection...")
    
    try:
        import psycopg2
        
        conn = psycopg2.connect(
            host='localhost',
            port='5433',
            database='banesco_test',
            user='postgres',
            password='postgres123'
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        print(f"✅ PostgreSQL connection successful: {version[0][:50]}...")
        return True
        
    except Exception as e:
        print(f"❌ PostgreSQL connection failed: {str(e)}")
        return False

def test_minio_connection():
    """Test MinIO connection"""
    print("\n🔍 Testing MinIO connection...")
    
    try:
        import requests
        
        # Test MinIO health endpoint
        response = requests.get('http://localhost:9000/minio/health/live', timeout=5)
        
        if response.status_code == 200:
            print("✅ MinIO connection successful")
            return True
        else:
            print(f"❌ MinIO connection failed: Status {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ MinIO connection failed: {str(e)}")
        return False

def test_spark_connection():
    """Test Spark connection"""
    print("\n🔍 Testing Spark connection...")
    
    try:
        import requests
        
        # Test Spark Master Web UI
        response = requests.get('http://localhost:8081', timeout=5)
        
        if response.status_code == 200:
            print("✅ Spark Master connection successful")
            return True
        else:
            print(f"❌ Spark Master connection failed: Status {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Spark Master connection failed: {str(e)}")
        return False

def test_airflow_connection():
    """Test Airflow connection"""
    print("\n🔍 Testing Airflow connection...")
    
    try:
        import requests
        
        # Test Airflow Web UI
        response = requests.get('http://localhost:8080/health', timeout=5)
        
        if response.status_code == 200:
            print("✅ Airflow Web UI connection successful")
            return True
        else:
            print(f"❌ Airflow Web UI connection failed: Status {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Airflow Web UI connection failed: {str(e)}")
        return False

def check_dag_syntax():
    """Check DAG syntax"""
    print("\n🔍 Checking DAG syntax...")
    
    try:
        # Import the DAG to check syntax
        sys.path.append('/opt/airflow/dags')
        
        # This would normally import the DAG, but we'll do a syntax check instead
        dag_file = '/opt/airflow/dags/data_bike_pipeline.py'
        
        if os.path.exists(dag_file):
            with open(dag_file, 'r') as f:
                content = f.read()
            
            # Basic syntax check
            compile(content, dag_file, 'exec')
            print("✅ DAG syntax is valid")
            return True
        else:
            print(f"❌ DAG file not found: {dag_file}")
            return False
            
    except SyntaxError as e:
        print(f"❌ DAG syntax error: {str(e)}")
        return False
    except Exception as e:
        print(f"❌ Error checking DAG: {str(e)}")
        return False

def run_test_script():
    """Run the test script from notebooks directory"""
    print("\n🚀 Running test script from notebooks directory...")
    
    try:
        # Change to notebooks directory
        notebooks_dir = '/Users/lincoln.prendergast/Documents/Repositorios/Banesco_Test_Data_Engineer/Cases use/Divvy_Bikes/notebooks'
        
        if os.path.exists(notebooks_dir):
            os.chdir(notebooks_dir)
            
            # Run the test script in test mode
            result = subprocess.run([
                'python', 'divvy_bikes_load_to_postgres.py', 
                '--test-mode', '--year-month', '2023-06'
            ], capture_output=True, text=True, timeout=60)
            
            if result.returncode == 0:
                print("✅ Test script executed successfully")
                print("📋 Output:")
                print(result.stdout[-500:])  # Show last 500 characters
                return True
            else:
                print(f"❌ Test script failed with return code {result.returncode}")
                print("📋 Error output:")
                print(result.stderr[-500:])  # Show last 500 characters
                return False
        else:
            print(f"❌ Notebooks directory not found: {notebooks_dir}")
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ Test script timed out")
        return False
    except Exception as e:
        print(f"❌ Error running test script: {str(e)}")
        return False

def main():
    """Main test function"""
    print("🧪 Divvy Bikes Data Pipeline - DAG Test Suite")
    print("=" * 60)
    
    tests = [
        ("Docker Services", check_docker_services),
        ("PostgreSQL Connection", test_postgres_connection),
        ("MinIO Connection", test_minio_connection),
        ("Spark Connection", test_spark_connection),
        ("Airflow Connection", test_airflow_connection),
        ("DAG Syntax", check_dag_syntax),
        ("Test Script", run_test_script)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n{'='*20} {test_name} {'='*20}")
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ {test_name} failed with exception: {str(e)}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "="*60)
    print("📊 TEST SUMMARY")
    print("="*60)
    
    passed = 0
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status}: {test_name}")
        if result:
            passed += 1
    
    print(f"\n📈 Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! DAG is ready to run.")
        return True
    else:
        print("⚠️  Some tests failed. Please fix issues before running the DAG.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
