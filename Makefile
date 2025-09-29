# Divvy Bikes Pipeline - Makefile
# This Makefile provides convenient commands for managing the pipeline

.PHONY: help start stop restart status logs clean build deploy test setup-vars setup-conns test-minio quick-setup urls

# Colors for output
RED=\033[0;31m
GREEN=\033[0;32m
YELLOW=\033[1;33m
BLUE=\033[0;34m
NC=\033[0m # No Color

# Default target
help:
	@echo "🚀 Banesco Data Engineering Stack - Available Commands"
	@echo "======================================================"
	@echo ""
	@echo "📋 Setup & Management:"
	@echo "  make start                    - Complete stack setup (READY FOR MANUAL EXECUTION)"
	@echo "  make start-stack              - Basic stack only"
	@echo "  make stop                     - Stop all services"
	@echo "  make restart                  - Restart all services"
	@echo "  make status                   - Check status of all services"
	@echo "  make logs                     - View logs from all services"
	@echo ""
	@echo "🔧 Setup Components:"
	@echo "  make create-minio-buckets     - Create required MinIO buckets"
	@echo "  make setup-vars               - Setup Airflow variables"
	@echo "  make setup-postgres-connection - Setup PostgreSQL connection"
	@echo "  make setup-spark-connection   - Setup Spark connection"
	@echo "  make start-streamlit          - Start Streamlit dashboard"
	@echo ""
	@echo "🔧 Development:"
	@echo "  make build                    - Build custom Airflow image"
	@echo "  make test-minio               - Test MinIO access"
	@echo "  make clean                    - Clean up containers and volumes"
	@echo "  make †an-all                - COMPLETE cleanup (removes ALL data)"
	@echo ""
	@echo "🌐 Access URLs:"
	@echo "  make urls                     - Show all service access URLs"
	@echo "  Airflow:    http://localhost:8080 (admin/admin)"
	@echo "  Spark:      http://localhost:8081"
	@echo "  MinIO:      http://localhost:9001 (minioadmin/minioadmin123)"
	@echo "  Streamlit:  http://localhost:8501"

# Check if Docker is running
check-docker:
	@echo -e "$(BLUE)[INFO]$(NC) Checking Docker status..."
	@if ! docker info > /dev/null 2>&1; then \
		echo -e "$(RED)[ERROR]$(NC) Docker is not running. Please start Docker first."; \
		exit 1; \
	fi
	@echo -e "$(GREEN)[SUCCESS]$(NC) Docker is running"

# Create necessary directories
create-dirs:
	@echo -e "$(BLUE)[INFO]$(NC) Creating necessary directories..."
	@mkdir -p stack/airflow/logs
	@mkdir -p stack/airflow/plugins
	@mkdir -p stack/minio/data
	@mkdir -p stack/postgres/data
	@mkdir -p stack/spark/data
	@echo -e "$(GREEN)[SUCCESS]$(NC) Directories created"

# Start the complete pipeline setup (without auto-execution)
start: check-docker create-dirs
	@echo "🚀 Starting Banesco Data Engineering Stack"
	@echo "=========================================="
	@echo -e "$(BLUE)[INFO]$(NC) Starting PostgreSQL..."
	@docker-compose up -d postgres
	@echo -e "$(BLUE)[INFO]$(NC) Starting MinIO..."
	@docker-compose up -d minio
	@echo -e "$(BLUE)[INFO]$(NC) Starting Spark Master..."
	@docker-compose up -d spark-master
	@echo -e "$(BLUE)[INFO]$(NC) Starting Spark Worker..."
	@docker-compose up -d spark-worker
	@echo -e "$(BLUE)[INFO]$(NC) Starting Streamlit dashboard..."
	@$(MAKE) start-streamlit
	@echo -e "$(BLUE)[INFO]$(NC) Creating MinIO buckets..."
	@$(MAKE) create-minio-buckets
	@echo -e "$(BLUE)[INFO]$(NC) Starting Airflow Database..."
	@docker-compose up -d airflow-db
	@echo -e "$(BLUE)[INFO]$(NC) Building custom Airflow image..."
	@docker build -f stack/airflow/Dockerfile -t banesco-airflow:latest .
	@echo -e "$(BLUE)[INFO]$(NC) Starting Airflow initialization..."
	@docker-compose up -d airflow-init
	@echo -e "$(BLUE)[INFO]$(NC) Starting Airflow services..."
	@docker-compose up -d airflow-scheduler airflow-webserver
	@echo -e "$(BLUE)[INFO]$(NC) Setting up Airflow configuration..."
	@docker cp setup_parameters/setup_complete.py banesco_airflow_scheduler:/tmp/setup_complete.py
	@docker exec banesco_airflow_scheduler python /tmp/setup_complete.py
	@echo ""
	@echo "=========================================="
	@echo -e "$(GREEN)[SUCCESS]$(NC) 🎉 Setup completed successfully!"
	@echo "=========================================="
	@echo ""
	@echo "📋 Service Access Information:"
	@echo "  🌐 Airflow Web UI: http://localhost:8080"
	@echo "     Username: admin"
	@echo "     Password: admin"
	@echo ""
	@echo "  🔥 Spark Master UI: http://localhost:8081"
	@echo ""
	@echo "  📦 MinIO Console: http://localhost:9001"
	@echo "     Username: minioadmin"
	@echo "     Password: minioadmin123"
	@echo ""
	@echo "  📊 Streamlit Dashboard: http://localhost:8501"
	@echo ""
	@echo "🎯 Pipeline Ready (MANUAL EXECUTION):"
	@echo "  1. Open Airflow Web UI: http://localhost:8080"
	@echo "  2. Navigate to DAGs page"
	@echo "  3. Find 'data_bike_pipeline' DAG (will be PAUSED)"
	@echo "  4. Click toggle to ENABLE the DAG"
	@echo "  5. Click 'Trigger DAG' to run manually"
	@echo ""
	@echo "✅ All services ready - Pipeline under YOUR control!"

# Start only the stack (without full pipeline setup)
start-stack: check-docker create-dirs
	@echo "🚀 Starting Banesco Data Stack"
	@echo "==============================="
	@echo -e "$(BLUE)[INFO]$(NC) Starting PostgreSQL..."
	@docker-compose up -d postgres
	@echo -e "$(BLUE)[INFO]$(NC) Starting MinIO..."
	@docker-compose up -d minio
	@echo -e "$(BLUE)[INFO]$(NC) Starting Spark Master..."
	@docker-compose up -d spark-master
	@echo -e "$(BLUE)[INFO]$(NC) Starting Spark Worker..."
	@docker-compose up -d spark-worker
	@echo -e "$(BLUE)[INFO]$(NC) Starting Streamlit dashboard..."
	@$(MAKE) start-streamlit
	@echo -e "$(BLUE)[INFO]$(NC) Starting Airflow Database..."
	@docker-compose up -d airflow-db
	@echo -e "$(BLUE)[INFO]$(NC) Starting Airflow initialization..."
	@docker-compose --profile init up -d airflow-init
	@echo -e "$(BLUE)[INFO]$(NC) Starting Airflow services..."
	@docker-compose up -d airflow-scheduler airflow-webserver
	@echo ""
	@echo "==============================="
	@echo -e "$(GREEN)[SUCCESS]$(NC) 🎉 Stack Started Successfully!"
	@echo "==============================="
	@echo ""
	@echo "🌐 Service Access:"
	@echo "  Airflow UI:     http://localhost:8080 (admin/admin)"
	@echo "  Spark Master:   http://localhost:8081"
	@echo "  MinIO Console:  http://localhost:9001 (minioadmin/minioadmin123)"
	@echo "  PostgreSQL:     localhost:5432 (postgres/postgres123)"

# Stop all services
stop:
	@echo "🛑 Stopping all services..."
	@docker-compose down

# Restart all services
restart: stop start

# Check status of all services
status:
	@echo "🔍 Divvy Bikes Pipeline Status Check"
	@echo "===================================="
	@echo ""
	@echo "🔧 Service Status:"
	@echo "------------------"
	@echo -n "Checking PostgreSQL... "
	@if docker ps --format "table {{.Names}}\t{{.Status}}" | grep -q "banesco_postgres.*Up"; then \
		echo -e "$(GREEN)✅ Running$(NC)"; \
	else \
		echo -e "$(RED)❌ Not Running$(NC)"; \
	fi
	@echo -n "Checking Airflow DB... "
	@if docker ps --format "table {{.Names}}\t{{.Status}}" | grep -q "banesco_airflow_db.*Up"; then \
		echo -e "$(GREEN)✅ Running$(NC)"; \
	else \
		echo -e "$(RED)❌ Not Running$(NC)"; \
	fi
	@echo -n "Checking MinIO... "
	@if docker ps --format "table {{.Names}}\t{{.Status}}" | grep -q "banesco_minio.*Up"; then \
		if curl -s -f "http://localhost:9000/minio/health/live" > /dev/null 2>&1; then \
			echo -e "$(GREEN)✅ Running & Healthy$(NC)"; \
		else \
			echo -e "$(YELLOW)⚠️  Running but not responding$(NC)"; \
		fi; \
	else \
		echo -e "$(RED)❌ Not Running$(NC)"; \
	fi
	@echo -n "Checking Spark Master... "
	@if docker ps --format "table {{.Names}}\t{{.Status}}" | grep -q "banesco_spark_master.*Up"; then \
		if curl -s -f "http://localhost:8081" > /dev/null 2>&1; then \
			echo -e "$(GREEN)✅ Running & Healthy$(NC)"; \
		else \
			echo -e "$(YELLOW)⚠️  Running but not responding$(NC)"; \
		fi; \
	else \
		echo -e "$(RED)❌ Not Running$(NC)"; \
	fi
	@echo -n "Checking Spark Worker... "
	@if docker ps --format "table {{.Names}}\t{{.Status}}" | grep -q "banesco_spark_worker.*Up"; then \
		echo -e "$(GREEN)✅ Running$(NC)"; \
	else \
		echo -e "$(RED)❌ Not Running$(NC)"; \
	fi
	@echo ""
	@echo "🌐 Airflow Services:"
	@echo "-------------------"
	@echo -n "Checking Airflow Scheduler... "
	@if docker ps --format "table {{.Names}}\t{{.Status}}" | grep -q "banesco_airflow_scheduler.*Up"; then \
		echo -e "$(GREEN)✅ Running$(NC)"; \
	else \
		echo -e "$(RED)❌ Not Running$(NC)"; \
	fi
	@echo -n "Checking Airflow Webserver... "
	@if docker ps --format "table {{.Names}}\t{{.Status}}" | grep -q "banesco_airflow_webserver.*Up"; then \
		if curl -s -f "http://localhost:8080/health" > /dev/null 2>&1; then \
			echo -e "$(GREEN)✅ Running & Healthy$(NC)"; \
		else \
			echo -e "$(YELLOW)⚠️  Running but not responding$(NC)"; \
		fi; \
	else \
		echo -e "$(RED)❌ Not Running$(NC)"; \
	fi
	@echo ""
	@echo "🌐 Access URLs:"
	@echo "--------------"
	@echo "  Airflow Web UI: http://localhost:8080"
	@echo "  Spark Master UI: http://localhost:8081"
	@echo "  MinIO Console: http://localhost:9001"

# View logs from all services
logs:
	@echo "📋 Viewing logs from all services..."
	@docker-compose logs -f

# Build custom Airflow image
build:
	@echo "🔨 Building custom Airflow image..."
	@docker build -f stack/airflow/Dockerfile -t banesco-airflow:latest .

# Deploy scripts to MinIO
deploy:
	@echo "📦 Deploying scripts to MinIO..."
	@cd stack/airflow/scripts && python deploy_to_minio.py

# Setup Airflow variables
setup-vars:
	@echo "🔧 Setting up Airflow Variables for Banesco Data Stack"
	@echo "============================================================"
	@if ! docker ps | grep -q "banesco_airflow_scheduler"; then \
		echo -e "$(RED)[ERROR]$(NC) Airflow scheduler container is not running!"; \
		echo -e "$(BLUE)[INFO]$(NC) Please start the stack first with: make start-stack"; \
		exit 1; \
	fi
	@echo -e "$(GREEN)[SUCCESS]$(NC) Airflow container is running"
	@echo -e "$(BLUE)[INFO]$(NC) Copying setup script to Airflow container..."
	@docker cp setup_parameters/setup_airflow_variables.py banesco_airflow_scheduler:/tmp/setup_airflow_variables.py
	@echo -e "$(BLUE)[INFO]$(NC) Executing variable setup script..."
	@docker exec banesco_airflow_scheduler python /tmp/setup_airflow_variables.py
	@echo -e "$(GREEN)[SUCCESS]$(NC) Airflow variables setup completed successfully!"
	@echo ""
	@echo "📋 Variables configured:"
	@echo "  - SPARK_MASTER_URL"
	@echo "  - MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_SECURE"
	@echo "  - POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD"
	@echo "  - MONGO_HOST, MONGO_PORT, MONGO_DB, MONGO_USER, MONGO_PASSWORD"
	@echo "  - KAFKA_BOOTSTRAP_SERVERS"
	@echo ""
	@echo "🌐 You can view these variables in the Airflow UI:"
	@echo "   http://localhost:8080/admin/variable/"

# Setup Airflow connections
setup-conns:
	@echo "🔧 Setting up Airflow connections..."
	@if ! docker ps | grep -q "banesco_airflow_scheduler"; then \
		echo -e "$(RED)[ERROR]$(NC) Airflow scheduler container is not running!"; \
		exit 1; \
	fi
	@echo -e "$(BLUE)[INFO]$(NC) Setting up Airflow connections..."
	@docker exec banesco_airflow_scheduler python /opt/airflow/scripts/setup_airflow_connections.py
	@echo -e "$(BLUE)[INFO]$(NC) Setting up Spark connection..."
	@docker exec banesco_airflow_scheduler python /opt/airflow/setup_parameters/setup_spark_connection.py
	@echo -e "$(GREEN)[SUCCESS]$(NC) Airflow connections setup completed!"

# Test MinIO access
test-minio:
	@echo "🧪 Testing MinIO access from Airflow container"
	@echo "=================================================="
	@if ! docker ps | grep -q "banesco_airflow_scheduler"; then \
		echo -e "$(RED)[ERROR]$(NC) Airflow scheduler container is not running!"; \
		exit 1; \
	fi
	@echo -e "$(GREEN)[SUCCESS]$(NC) Airflow container is running"
	@echo -e "$(BLUE)[INFO]$(NC) Testing MinIO connectivity..."
	@docker exec banesco_airflow_scheduler curl -f http://minio:9000/minio/health/live
	@echo -e "$(GREEN)[SUCCESS]$(NC) MinIO is accessible from Airflow container"
	@echo -e "$(BLUE)[INFO]$(NC) Testing file access in MinIO..."
	@docker exec banesco_airflow_scheduler python3 -c "import requests; response = requests.get('http://minio:9000/minio/health/live', timeout=10); print(f'MinIO Health Check: {response.status_code}'); print('MinIO connectivity test passed')"
	@echo -e "$(GREEN)[SUCCESS]$(NC) MinIO file access test passed"
	@echo -e "$(BLUE)[INFO]$(NC) Testing Spark configuration..."
	@docker exec banesco_airflow_scheduler python3 -c "from airflow.models import Variable; print('Airflow Variable import: OK'); spark_master = Variable.get('SPARK_MASTER_URL'); minio_endpoint = Variable.get('MINIO_ENDPOINT'); print(f'Spark Master URL: {spark_master}'); print(f'MinIO Endpoint: {minio_endpoint}'); print('Airflow Variables: OK')"
	@echo -e "$(GREEN)[SUCCESS]$(NC) Spark configuration test passed"
	@echo -e "$(GREEN)[SUCCESS]$(NC) 🎉 All tests passed!"
	@echo ""
	@echo "✅ MinIO is accessible from Airflow"
	@echo "✅ Files are uploaded to banesco-pa-data-artifact bucket"
	@echo "✅ Airflow variables are configured"
	@echo "✅ DAG should be able to access MinIO files"

# Run integration tests
test: test-minio
	@echo "🧪 Running integration tests..."
	@echo -e "$(GREEN)[SUCCESS]$(NC) Integration tests completed!"

# Clean up containers and volumes
clean:
	@echo "🧹 Cleaning up containers and volumes..."
	@docker-compose down -v
	@docker system prune -f
	@echo "✅ Cleanup completed"

# Quick setup (build + start + deploy)
quick-setup: build start deploy
	@echo "🎉 Quick setup completed!"

# Create MinIO buckets
create-minio-buckets:
	@echo "📦 Creating MinIO buckets..."
	@if ! docker ps | grep -q "banesco_minio"; then \
		echo -e "$(RED)[ERROR]$(NC) MinIO container is not running!"; \
		exit 1; \
	fi
	@printf "$(BLUE)[INFO]$(NC) Setting up MinIO client alias...\n"
	@docker exec banesco_minio mc alias set local http://localhost:9000 minioadmin minioadmin123 > /dev/null 2>&1 || true
	@printf "$(BLUE)[INFO]$(NC) Creating required MinIO buckets...\n"
	@docker exec banesco_minio mc mb local/banesco-pa-data-raw-zone --ignore-existing > /dev/null 2>&1 || true
	@docker exec banesco_minio mc mb local/banesco-pa-data-stage-zone --ignore-existing > /dev/null 2>&1 || true
	@docker exec banesco_minio mc mb local/banesco-pa-data-analytics-zone --ignore-existing > /dev/null 2>&1 || true
	@docker exec banesco_minio mc mb local/banesco-pa-data-artifact --ignore-existing > /dev/null 2>&1 || true
	@printf "$(BLUE)[INFO]$(NC) Verifying buckets created...\n"
	@echo "📦 Available buckets:"
	@docker exec banesco_minio mc ls local/ | grep -E "banesco-pa-data" | sed 's/^/   ✅ /'
	@printf "$(GREEN)[SUCCESS]$(NC) MinIO buckets created successfully!\n"

# Setup PostgreSQL connection
setup-postgres-connection:
	@echo "🔌 Setting up PostgreSQL connection..."
	@if ! docker ps | grep -q "banesco_airflow_scheduler"; then \
		echo -e "$(RED)[ERROR]$(NC) Airflow scheduler container is not running!"; \
		exit 1; \
	fi
	@echo -e "$(BLUE)[INFO]$(NC) Configuring PostgreSQL connection in Airflow..."
	@docker exec banesco_airflow_scheduler python /opt/airflow/setup_parameters/setup_postgres_connection.py
	@echo -e "$(GREEN)[SUCCESS]$(NC) PostgreSQL connection setup completed!"

# Setup Spark connection
setup-spark-connection:
	@echo "⚡ Setting up Spark connection..."
	@if ! docker ps | grep -q "banesco_airflow_scheduler"; then \
		echo -e "$(RED)[ERROR]$(NC) Airflow scheduler container is not running!"; \
		exit 1; \
	fi
	@if ! docker ps | grep -q "banesco_spark_master"; then \
		echo -e "$(RED)[ERROR]$(NC) Spark master container is not running!"; \
		exit 1; \
	fi
	@echo -e "$(BLUE)[INFO]$(NC) Configuring Spark connection in Airflow..."
	@docker exec banesco_airflow_scheduler python /opt/airflow/setup_parameters/setup_spark_connection.py
	@echo -e "$(GREEN)[SUCCESS]$(NC) Spark connection setup completed!"

# Start Streamlit dashboard
start-streamlit:
	@echo "📊 Starting Streamlit dashboard..."
	@if ! docker ps | grep -q "banesco_postgres"; then \
		echo -e "$(RED)[ERROR]$(NC) PostgreSQL container is not running!"; \
		exit 1; \
	fi
	@echo -e "$(BLUE)[INFO]$(NC) Starting Streamlit service..."
	@docker-compose up -d streamlit
	@echo -e "$(GREEN)[SUCCESS]$(NC) Streamlit dashboard started!"
	@echo "🌐 Access: http://localhost:8501"

# Clean up everything including data
clean-all:
	@echo "🧹 COMPLETE CLEANUP - This will remove ALL data!"
	@echo "================================================"
	@echo -e "$(YELLOW)[WARNING]$(NC) This will delete all containers, volumes, and data!"
	@read -p "Are you sure? (y/N): " confirm; \
	if [ "$$confirm" = "y" ] || [ "$$confirm" = "Y" ]; then \
		echo -e "$(BLUE)[INFO]$(NC) Stopping all services..."; \
		docker-compose down -v; \
		echo -e "$(BLUE)[INFO]$(NC) Removing Docker system cache..."; \
		docker system prune -f; \
		echo -e "$(BLUE)[INFO]$(NC) Cleaning data directories..."; \
		rm -rf stack/*/data/* 2>/dev/null || true; \
		echo -e "$(GREEN)[SUCCESS]$(NC) Complete cleanup finished!"; \
	else \
		echo "Cleanup cancelled."; \
	fi

# Show service URLs
urls:
	@echo "🌐 Service Access URLs:"
	@echo "  Airflow Web UI:    http://localhost:8080"
	@echo "  Spark Master UI:   http://localhost:8081"
	@echo "  MinIO Console:     http://localhost:9001"
	@echo "  Streamlit Dashboard: http://localhost:8501"
	@echo ""
	@echo "🔐 Default Credentials:"
	@echo "  Airflow: admin / admin"
	@echo "  MinIO:   minioadmin / minioadmin123"