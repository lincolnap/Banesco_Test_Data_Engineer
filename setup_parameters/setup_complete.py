#!/usr/bin/env python3
"""
Complete Airflow Setup Script
Executes all necessary setup scripts for the Divvy Bikes pipeline
"""

import subprocess
import sys
import time
import os

def wait_for_airflow_ready(max_retries=60, delay=5):
    """Wait for Airflow webserver to be ready"""
    print("⏳ Waiting for Airflow webserver to be ready...")
    
    for attempt in range(max_retries):
        try:
            import requests
            response = requests.get("http://localhost:8080/health", timeout=5)
            if response.status_code == 200:
                print("✅ Airflow webserver is ready!")
                return True
        except Exception:
            pass
        
        if attempt < max_retries - 1:
            print(f"⏳ Attempt {attempt + 1}/{max_retries} - Airflow not ready yet, waiting {delay}s...")
            time.sleep(delay)
        else:
            print(f"❌ Airflow not ready after {max_retries} attempts")
            return False
    
    return False

def run_setup_script(script_name, description):
    """Run a setup script and handle errors"""
    print(f"\n🚀 {description}")
    print("=" * 50)
    
    try:
        result = subprocess.run([
            sys.executable, 
            f"/opt/airflow/setup_parameters/{script_name}"
        ], capture_output=True, text=True, timeout=300)
        
        if result.returncode == 0:
            print(f"✅ {description} completed successfully!")
            if result.stdout:
                print("Output:")
                print(result.stdout)
            return True
        else:
            print(f"❌ {description} failed!")
            if result.stderr:
                print("Error:")
                print(result.stderr)
            if result.stdout:
                print("Output:")
                print(result.stdout)
            return False
            
    except subprocess.TimeoutExpired:
        print(f"❌ {description} timed out after 5 minutes!")
        return False
    except Exception as e:
        print(f"❌ Error running {description}: {str(e)}")
        return False

def main():
    """Main setup function"""
    print("🎯 Complete Airflow Setup for Divvy Bikes Pipeline")
    print("=" * 60)
    
    # Wait for Airflow to be ready
    if not wait_for_airflow_ready():
        print("❌ Airflow is not available. Exiting...")
        sys.exit(1)
    
    # List of setup scripts to run
    setup_scripts = [
        ("setup_airflow_variables.py", "Setting up Airflow Variables"),
        ("setup_spark_connection.py", "Setting up Spark Connection"),
        ("setup_postgres_connection.py", "Setting up PostgreSQL Connection")
    ]
    
    success_count = 0
    total_scripts = len(setup_scripts)
    
    # Run each setup script
    for script_name, description in setup_scripts:
        if run_setup_script(script_name, description):
            success_count += 1
        else:
            print(f"⚠️ Continuing with other scripts despite {description} failure...")
    
    # Summary
    print(f"\n📊 Setup Summary")
    print("=" * 30)
    print(f"✅ Successful: {success_count}/{total_scripts}")
    print(f"❌ Failed: {total_scripts - success_count}/{total_scripts}")
    
    if success_count == total_scripts:
        print("\n🎉 All setup scripts completed successfully!")
        print("🚀 Airflow is ready for the Divvy Bikes pipeline!")
        sys.exit(0)
    else:
        print(f"\n⚠️ {total_scripts - success_count} setup script(s) failed!")
        print("🔧 Please check the logs above for details.")
        sys.exit(1)

if __name__ == "__main__":
    main()
