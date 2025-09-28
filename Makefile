# Banesco Data Engineering Stack - Makefile
# This Makefile provides convenient commands to manage the Docker stack

.PHONY: help up down build logs ps recreate clean init up-svc down-svc logs-svc restart-svc status health

# Default target
help: ## Show this help message
	@echo "Banesco Data Engineering Stack - Available Commands:"
	@echo "=================================================="
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# Main stack operations
up: ## Start the entire stack in detached mode
	@echo "🚀 Starting Banesco Data Engineering Stack..."
	docker compose up -d
	@echo "✅ Stack started successfully!"
	@echo "📊 Dashboard available at: http://localhost:8502"
	@echo "🔧 Airflow UI available at: http://localhost:8080 (admin/admin)"
	@echo "📈 Spark Master UI available at: http://localhost:8081"
	@echo "💾 MinIO Console available at: http://localhost:9001 (minioadmin/minioadmin123)"

down: ## Stop the entire stack
	@echo "🛑 Stopping Banesco Data Engineering Stack..."
	docker compose down
	@echo "✅ Stack stopped successfully!"

build: ## Build all custom images
	@echo "🔨 Building custom images..."
	docker compose build
	@echo "✅ Build completed!"

restart: ## Restart the entire stack
	@echo "🔄 Restarting Banesco Data Engineering Stack..."
	docker compose restart
	@echo "✅ Stack restarted successfully!"

# Logs and monitoring
logs: ## Show logs from all services
	docker compose logs -f

logs-svc: ## Show logs from a specific service (usage: make logs-svc svc=postgres)
	@if [ -z "$(svc)" ]; then \
		echo "❌ Please specify a service name: make logs-svc svc=<service_name>"; \
		echo "Available services: postgres, airflow-db, kafka, mongodb, minio, airflow-webserver, airflow-scheduler, spark-master, spark-worker, streamlit"; \
	else \
		docker compose logs -f $(svc); \
	fi

ps: ## Show status of all containers
	@echo "📋 Container Status:"
	docker compose ps

status: ps ## Alias for ps command

health: ## Check health status of all services
	@echo "🏥 Health Check Status:"
	@echo "======================"
	@for service in postgres airflow-db kafka mongodb minio airflow-webserver spark-master streamlit; do \
		echo -n "$$service: "; \
		if docker compose ps $$service | grep -q "healthy\|Up"; then \
			echo "✅ Healthy"; \
		else \
			echo "❌ Unhealthy/Stopped"; \
		fi; \
	done

# Service-specific operations
up-svc: ## Start a specific service (usage: make up-svc svc=postgres)
	@if [ -z "$(svc)" ]; then \
		echo "❌ Please specify a service name: make up-svc svc=<service_name>"; \
		echo "Available services: postgres, airflow-db, kafka, mongodb, minio, airflow-webserver, airflow-scheduler, spark-master, spark-worker, streamlit"; \
	else \
		echo "🚀 Starting $(svc) service..."; \
		docker compose up -d $(svc); \
		echo "✅ $(svc) service started!"; \
	fi

down-svc: ## Stop a specific service (usage: make down-svc svc=postgres)
	@if [ -z "$(svc)" ]; then \
		echo "❌ Please specify a service name: make down-svc svc=<service_name>"; \
		echo "Available services: postgres, airflow-db, kafka, mongodb, minio, airflow-webserver, airflow-scheduler, spark-master, spark-worker, streamlit"; \
	else \
		echo "🛑 Stopping $(svc) service..."; \
		docker compose stop $(svc); \
		echo "✅ $(svc) service stopped!"; \
	fi

restart-svc: ## Restart a specific service (usage: make restart-svc svc=postgres)
	@if [ -z "$(svc)" ]; then \
		echo "❌ Please specify a service name: make restart-svc svc=<service_name>"; \
		echo "Available services: postgres, airflow-db, kafka, mongodb, minio, airflow-webserver, airflow-scheduler, spark-master, spark-worker, streamlit"; \
	else \
		echo "🔄 Restarting $(svc) service..."; \
		docker compose restart $(svc); \
		echo "✅ $(svc) service restarted!"; \
	fi

# Initialization
init: ## Initialize Airflow database and create admin user
	@echo "🔧 Initializing Airflow database..."
	docker run --rm --network banesco_test \
		-e AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow_user:airflow_password123@airflow-db:5432/airflow_db \
		-e AIRFLOW__CORE__FERNET_KEY=ndc7_H8h7pbNlAhm2oVQ6WHWR5YxH4k-mVLkhCpntao= \
		apache/airflow:2.9.3 airflow db init
	@echo "👤 Creating Airflow admin user..."
	docker run --rm --network banesco_test \
		-e AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow_user:airflow_password123@airflow-db:5432/airflow_db \
		-e AIRFLOW__CORE__FERNET_KEY=ndc7_H8h7pbNlAhm2oVQ6WHWR5YxH4k-mVLkhCpntao= \
		apache/airflow:2.9.3 airflow users create \
		--username admin --firstname Admin --lastname User \
		--role Admin --email admin@example.com --password admin
	@echo "✅ Airflow initialization completed!"
	@echo "🔑 Login credentials: admin/admin"

# Cleanup operations
clean: ## Stop stack and remove containers, networks, and volumes (WARNING: This will delete all data!)
	@echo "⚠️  WARNING: This will delete ALL data in the stack!"
	@read -p "Are you sure? Type 'yes' to continue: " confirm; \
	if [ "$$confirm" = "yes" ]; then \
		echo "🧹 Cleaning up stack..."; \
		docker compose down -v --remove-orphans; \
		docker system prune -f; \
		echo "✅ Cleanup completed!"; \
	else \
		echo "❌ Cleanup cancelled."; \
	fi

recreate: ## Stop stack, remove volumes, and start fresh (WARNING: This will delete all data!)
	@echo "⚠️  WARNING: This will delete ALL data and start fresh!"
	@read -p "Are you sure? Type 'yes' to continue: " confirm; \
	if [ "$$confirm" = "yes" ]; then \
		echo "🔄 Recreating stack with fresh data..."; \
		docker compose down -v --remove-orphans; \
		docker compose up -d; \
		echo "✅ Stack recreated successfully!"; \
	else \
		echo "❌ Recreate cancelled."; \
	fi

# Utility commands
shell-postgres: ## Open a PostgreSQL shell (main database)
	@echo "🐘 Opening PostgreSQL shell (main database)..."
	docker compose exec postgres psql -U postgres -d banesco_test

shell-airflow-db: ## Open Airflow PostgreSQL shell
	@echo "🐘 Opening Airflow PostgreSQL shell..."
	docker compose exec airflow-db psql -U airflow_user -d airflow_db

shell-mongodb: ## Open a MongoDB shell
	@echo "🍃 Opening MongoDB shell..."
	docker compose exec mongodb mongosh -u admin -p admin123

shell-kafka: ## Open a Kafka shell
	@echo "📨 Opening Kafka shell..."
	docker compose exec kafka bash

shell-airflow: ## Open an Airflow shell
	@echo "🌪️ Opening Airflow shell..."
	docker compose exec airflow-webserver bash

# Development helpers
dev-up: ## Start stack for development (with logs)
	@echo "🔧 Starting stack for development..."
	docker compose up

dev-down: ## Stop development stack
	@echo "🛑 Stopping development stack..."
	docker compose down

# Backup and restore (basic)
backup-postgres: ## Backup main PostgreSQL data
	@echo "💾 Creating main PostgreSQL backup..."
	@mkdir -p backups
	docker compose exec postgres pg_dump -U postgres -d banesco_test > backups/postgres_backup_$(shell date +%Y%m%d_%H%M%S).sql
	@echo "✅ Main PostgreSQL backup created!"

backup-airflow-db: ## Backup Airflow PostgreSQL data
	@echo "💾 Creating Airflow PostgreSQL backup..."
	@mkdir -p backups
	docker compose exec airflow-db pg_dump -U airflow_user -d airflow_db > backups/airflow_db_backup_$(shell date +%Y%m%d_%H%M%S).sql
	@echo "✅ Airflow PostgreSQL backup created!"

backup-mongodb: ## Backup MongoDB data
	@echo "💾 Creating MongoDB backup..."
	@mkdir -p backups
	docker compose exec mongodb mongodump --username admin --password admin123 --authenticationDatabase admin --out /tmp/backup
	docker compose cp mongodb:/tmp/backup ./backups/mongodb_backup_$(shell date +%Y%m%d_%H%M%S)
	@echo "✅ MongoDB backup created!"

# Quick access URLs
urls: ## Show all service URLs
	@echo "🌐 Service URLs:"
	@echo "================"
	@echo "📊 Streamlit Dashboard: http://localhost:8502"
	@echo "🔧 Airflow UI: http://localhost:8080 (admin/admin)"
	@echo "📈 Spark Master UI: http://localhost:8081"
	@echo "💾 MinIO Console: http://localhost:9001 (minioadmin/minioadmin123)"
	@echo "🐘 PostgreSQL (main): localhost:5433 (postgres/postgres123)"
	@echo "🐘 PostgreSQL (Airflow): localhost:5434 (airflow_user/airflow_password123)"
	@echo "🍃 MongoDB: localhost:27017 (admin/admin123)"
	@echo "📨 Kafka: localhost:9092"
	@echo "📨 Kafka External: localhost:9094"

# Show current configuration
config: ## Show current Docker Compose configuration
	@echo "⚙️  Current Docker Compose Configuration:"
	@echo "========================================"
	docker compose config
