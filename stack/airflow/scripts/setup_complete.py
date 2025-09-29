"""
Complete Setup Script for Divvy Bikes Pipeline
This script sets up everything needed for the Airflow + Spark + MinIO integration
"""

import os
import sys
import subprocess
import time
from pathlib import Path


def run_command(command, description):
    """Run a command and handle errors"""
    print(f"🔧 {description}...")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"✅ {description} completed successfully")
        if result.stdout:
            print(f"📝 Output: {result.stdout.strip()}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed: {e}")
        if e.stderr:
            print(f"📝 Error: {e.stderr.strip()}")
        return False


def check_docker_running():
    """Check if Docker is running"""
    print("🔍 Checking Docker status...")
    try:
        result = subprocess.run("docker ps", shell=True, check=True, capture_output=True, text=True)
        print("✅ Docker is running")
        return True
    except subprocess.CalledProcessError:
        print("❌ Docker is not running. Please start Docker first.")
        return False


def setup_environment():
    """Set up environment variables and configuration"""
    print("🔧 Setting up environment...")
    
    # Create .env file for Airflow if it doesn't exist
    env_file = Path("../.env")
    if not env_file.exists():
        env_content = """# Airflow Configuration
AIRFLOW_UID=50000
AIRFLOW_GID=0
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://postgres:postgres123@airflow-db:5432/airflow
AIRFLOW__CORE__FERNET_KEY=your-fernet-key-here
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true
AIRFLOW__CORE__LOAD_EXAMPLES=false
AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth
AIRFLOW__WEBSERVER__EXPOSE_CONFIG=true
AIRFLOW__WEBSERVER__EXPOSE_HOSTNAME=true
AIRFLOW__WEBSERVER__EXPOSE_STACKTRACE=true
AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK=true
AIRFLOW__LOGGING__LOGGING_LEVEL=INFO
AIRFLOW__LOGGING__FAB_LOGGING_LEVEL=WARN

# MinIO Configuration
MINIO_ENDPOINT=minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin123

# Spark Configuration
SPARK_MASTER_URL=spark://spark-master:7077
"""
        with open(env_file, 'w') as f:
            f.write(env_content)
        print("✅ Created .env file")
    else:
        print("ℹ️  .env file already exists")
    
    return True


def build_airflow_image():
    """Build custom Airflow Docker image"""
    print("🔧 Building custom Airflow image...")
    
    # Change to the airflow directory
    os.chdir(Path(__file__).parent)
    
    command = "docker build -t banesco-airflow:latest ."
    return run_command(command, "Building Airflow image")


def start_services():
    """Start all required services"""
    print("🚀 Starting services...")
    
    # Change to project root
    os.chdir(Path(__file__).parent.parent.parent)
    
    # Start services
    command = "docker-compose up -d postgres airflow-db minio spark-master spark-worker"
    if not run_command(command, "Starting core services"):
        return False
    
    # Wait for services to be ready
    print("⏳ Waiting for services to be ready...")
    time.sleep(30)
    
    # Start Airflow services
    command = "docker-compose up -d airflow-init"
    if not run_command(command, "Starting Airflow initialization"):
        return False
    
    # Wait for Airflow init
    print("⏳ Waiting for Airflow initialization...")
    time.sleep(60)
    
    # Start Airflow scheduler and webserver
    command = "docker-compose up -d airflow-scheduler airflow-webserver"
    return run_command(command, "Starting Airflow services")


def deploy_to_minio():
    """Deploy scripts and DAGs to MinIO"""
    print("📦 Deploying to MinIO...")
    
    # Change to scripts directory
    os.chdir(Path(__file__).parent)
    
    # Install boto3 if not available
    command = "pip install boto3 requests"
    run_command(command, "Installing required packages")
    
    # Run deployment script
    command = "python deploy_to_minio.py"
    return run_command(command, "Deploying to MinIO")


def setup_airflow_connections():
    """Set up Airflow connections and variables"""
    print("🔧 Setting up Airflow connections...")
    
    # Wait for Airflow to be ready
    print("⏳ Waiting for Airflow to be ready...")
    time.sleep(30)
    
    # Run setup script inside Airflow container
    command = "docker exec banesco_airflow_scheduler python /opt/airflow/scripts/setup_airflow_connections.py"
    if not run_command(command, "Setting up Airflow connections"):
        return False
    
    # Setup Spark connection
    print("🔧 Setting up Spark connection...")
    command = "docker exec banesco_airflow_scheduler python /opt/airflow/scripts/setup_spark_connection.py"
    return run_command(command, "Setting up Spark connection")


def run_integration_test():
    """Run integration test"""
    print("🧪 Running integration test...")
    
    # Change to scripts directory
    os.chdir(Path(__file__).parent)
    
    # Run test script
    command = "python test_integration.py"
    return run_command(command, "Running integration test")


def show_access_info():
    """Show access information for services"""
    print("\n" + "=" * 60)
    print("🎉 SETUP COMPLETED SUCCESSFULLY!")
    print("=" * 60)
    print("\n📋 Service Access Information:")
    print("  🌐 Airflow Web UI: http://localhost:8080")
    print("     Username: admin")
    print("     Password: admin")
    print("\n  🔥 Spark Master UI: http://localhost:8081")
    print("  📦 MinIO Console: http://localhost:9001")
    print("     Username: minioadmin")
    print("     Password: minioadmin123")
    print("\n📊 Bucket Information:")
    print("  📁 Scripts & DAGs: banesco-pa-data-artifact")
    print("  📁 Data Output: banesco-pa-data-raw-zone")
    print("\n🚀 Next Steps:")
    print("  1. Open Airflow Web UI")
    print("  2. Enable the 'data_bike_pipeline' DAG")
    print("  3. Trigger the DAG manually or wait for scheduled execution")
    print("\n📝 DAG Information:")
    print("  - Name: data_bike_pipeline")
    print("  - Schedule: Daily")
    print("  - Tasks: extract → transform → load → report → cleanup")
    print("=" * 60)


def main():
    """Main setup function"""
    print("🚀 Divvy Bikes Pipeline Complete Setup")
    print("Setting up: Airflow + Spark + MinIO Integration")
    print("=" * 60)
    
    # Check prerequisites
    if not check_docker_running():
        sys.exit(1)
    
    # Setup steps
    steps = [
        ("Environment Setup", setup_environment),
        ("Building Airflow Image", build_airflow_image),
        ("Starting Services", start_services),
        ("Deploying to MinIO", deploy_to_minio),
        ("Setting up Airflow Connections", setup_airflow_connections),
        ("Running Integration Test", run_integration_test)
    ]
    
    for step_name, step_func in steps:
        print(f"\n📋 Step: {step_name}")
        print("-" * 40)
        
        if not step_func():
            print(f"\n❌ Setup failed at step: {step_name}")
            print("🔧 Please check the errors above and try again.")
            sys.exit(1)
        
        print(f"✅ {step_name} completed successfully")
    
    # Show access information
    show_access_info()


if __name__ == "__main__":
    main()
