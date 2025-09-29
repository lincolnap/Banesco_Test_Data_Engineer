# 🏦 Banesco Data Engineering Stack

A comprehensive data engineering stack for Banesco, including PostgreSQL, MongoDB, Kafka, MinIO, Apache Airflow, Apache Spark, and Streamlit dashboard.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Services](#services)
- [Configuration](#configuration)
- [Usage](#usage)
- [Troubleshooting](#troubleshooting)
- [Development](#development)
- [Contributing](#contributing)
- [License](#license)

## 🎯 Overview

This project provides a complete data engineering infrastructure using Docker containers, orchestrated with Docker Compose. The stack includes:

- **PostgreSQL 16**: Primary relational database
- **MongoDB 7**: Document database
- **Apache Kafka + Zookeeper**: Message streaming platform
- **MinIO**: S3-compatible object storage
- **Apache Airflow 2.9**: Workflow orchestration
- **Apache Spark 3.5**: Big data processing
- **Streamlit**: Interactive dashboard and monitoring

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Streamlit     │    │    Airflow      │    │      Spark      │
│   Dashboard     │    │   Scheduler     │    │     Master      │
│   Port: 8501    │    │   Port: 8080    │    │   Port: 8081    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────┐
                    │   banesco_test  │
                    │     Network     │
                    └─────────────────┘
                                 │
    ┌─────────────┬─────────────┬─────────────┬─────────────┐
    │ PostgreSQL  │   MongoDB   │    Kafka    │    MinIO    │
    │ Port: 5432  │ Port: 27017 │ Port: 9092  │ Port: 9000  │
    └─────────────┴─────────────┴─────────────┴─────────────┘
```

## 🔧 Prerequisites

Before running this stack, ensure you have the following installed:

- **Docker** (version 20.10 or higher)
- **Docker Compose** (version 2.0 or higher)
- **Make** (for convenient commands)
- **Git** (for version control)

### Verify Installation

```bash
docker --version
docker compose version
make --version
```

## 🚀 Quick Start

### Option 1: Complete Pipeline Setup (Recommended)

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd Banaesco_test_dataengineer
   ```

2. **Start the Divvy Bikes Pipeline**
   ```bash
   make start
   ```

3. **Access the services**
   - 🔧 **Airflow UI**: http://localhost:8080 (admin/admin)
   - 📈 **Spark Master**: http://localhost:8081
   - 💾 **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin123)
   - 📊 **Dashboard**: http://localhost:8501

### Option 2: Basic Stack Setup

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd Banaesco_test_dataengineer
   ```

2. **Start the stack**
   ```bash
   make start-stack
   ```

3. **Access the services**
   - 📊 **Dashboard**: http://localhost:8501
   - 🔧 **Airflow UI**: http://localhost:8080 (admin/admin)
   - 📈 **Spark Master**: http://localhost:8081
   - 💾 **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin123)

## 🔧 Services

### PostgreSQL
- **Purpose**: Primary relational database for Airflow metadata and application data
- **Port**: 5432
- **Default Credentials**: postgres/postgres123
- **Databases**: airflow_db, banesco_analytics, banesco_warehouse

### MongoDB
- **Purpose**: Document database for flexible data storage
- **Port**: 27017
- **Default Credentials**: admin/admin123
- **Database**: banesco_test

### Apache Kafka
- **Purpose**: Real-time data streaming and event processing
- **Port**: 9092
- **Dependencies**: Zookeeper (port 2181)
- **Features**: Auto-topic creation enabled

### MinIO
- **Purpose**: S3-compatible object storage for data lakes
- **Ports**: 9000 (API), 9001 (Console)
- **Default Credentials**: minioadmin/minioadmin123

### Apache Airflow
- **Purpose**: Workflow orchestration and data pipeline management
- **Port**: 8080
- **Default Credentials**: admin/admin
- **Executor**: LocalExecutor
- **Database**: PostgreSQL

### Apache Spark
- **Purpose**: Big data processing and analytics
- **Master Port**: 7077, Web UI: 8081
- **Worker Port**: 8082
- **Mode**: Standalone cluster

### Streamlit
- **Purpose**: Interactive dashboard and monitoring
- **Port**: 8501
- **Features**: Real-time metrics, service monitoring, data connectors

## ⚙️ Configuration

### Environment Variables

Each service has its own `.env` file in the `stack/<service>/` directory. Copy the `.env.example` files and modify as needed:

```bash
# Example for PostgreSQL
cp stack/postgres/.env.example stack/postgres/.env
# Edit the .env file with your preferred settings
```

### Custom Configuration

- **PostgreSQL**: Modify `stack/postgres/init/` for custom initialization scripts
- **Airflow**: Add DAGs to `stack/airflow/dags/`
- **Streamlit**: Customize `stack/streamlit/app/app.py`
- **Spark**: Add configuration files to `stack/spark/config/`

## 📖 Usage

### Make Commands

The project includes a comprehensive Makefile with the following commands:

#### Basic Operations
```bash
make start       # Start the complete pipeline (recommended)
make start-stack # Start only the stack (without full setup)
make stop        # Stop all services
make restart     # Restart all services
make build       # Build custom Airflow image
make clean       # Clean up containers and volumes
```

#### Pipeline Management
```bash
make deploy      # Deploy scripts to MinIO
make setup-vars  # Setup Airflow variables
make setup-conns # Setup Airflow connections
make test        # Run integration tests
make test-minio  # Test MinIO access
```

#### Monitoring and Debugging
```bash
make status      # Check status of all services
make logs        # View logs from all services
make urls        # Show all service URLs
```

#### Quick Setup
```bash
make quick-setup # Build + start + deploy (complete setup)
make help        # Show all available commands
```

### Manual Docker Compose Commands

If you prefer using Docker Compose directly:

```bash
# Start services
docker compose up -d

# View logs
docker compose logs -f

# Stop services
docker compose down

# Scale services
docker compose up -d --scale spark-worker=3
```

## 🔍 Troubleshooting

### Common Issues

#### Port Conflicts
If you encounter port conflicts, check which services are using the ports:

```bash
# Check port usage
lsof -i :5432  # PostgreSQL
lsof -i :9092  # Kafka
lsof -i :8080  # Airflow
```

#### Permission Issues
If you encounter permission issues with volumes:

```bash
# Fix ownership (Linux/macOS)
sudo chown -R $USER:$USER stack/
```

#### Service Health Issues
Check service health and logs:

```bash
make health
make logs-svc svc=<service_name>
```

#### Kafka Connection Issues
If external clients can't connect to Kafka:

1. Check the `KAFKA_ADVERTISED_LISTENERS` in `stack/kafka/.env`
2. Ensure the advertised listener matches your client's connection string
3. For local connections, use `localhost:9092`

#### Airflow Initialization Issues
If Airflow fails to start:

1. Ensure PostgreSQL is healthy: `make logs-svc svc=postgres`
2. Initialize Airflow database: `make init`
3. Check Airflow logs: `make logs-svc svc=airflow-webserver`

### Reset Everything
To start completely fresh (⚠️ **This will delete all data**):

```bash
make recreate
```

## 🛠️ Development

### Adding New Services

1. Create service directory: `mkdir -p stack/new-service`
2. Add `.env.example` and `.env` files
3. Update `docker-compose.yml`
4. Add Makefile targets if needed

### Custom DAGs

Add your Airflow DAGs to `stack/airflow/dags/`. The project includes:

#### Divvy Bikes Pipeline (`data_bike_pipeline.py`)
- **Extract**: Downloads Divvy Bikes data using Spark
- **Transform**: Data processing and quality checks
- **Load**: Saves data to MinIO in Parquet format
- **Report**: Generates analytics and summaries
- **Cleanup**: Removes temporary files

#### Example DAG (`example_dag.py`)
- Basic data extraction
- Transformation
- Loading to PostgreSQL
- Report generation

### Pipeline Management

The Divvy Bikes pipeline includes comprehensive management tools:

```bash
# Check pipeline status
make status

# Deploy scripts to MinIO
make deploy

# Run integration tests
make test

# Setup Airflow connections
make setup-conns
```

### Extending Streamlit Dashboard

Modify `stack/streamlit/app/app.py` to add:
- New data sources
- Custom visualizations
- Additional monitoring features

### Backup and Restore

```bash
# Backup PostgreSQL
make backup-postgres

# Backup MongoDB
make backup-mongodb
```

## 📊 Monitoring

### Health Checks

All services include health checks. Monitor them with:

```bash
make health
```

### Logs

View logs for debugging:

```bash
# All services
make logs

# Specific service
make logs-svc svc=postgres
```

### Metrics

Access service-specific monitoring:
- **Airflow**: Built-in metrics in the UI
- **Spark**: Master and Worker UIs
- **Streamlit**: Custom dashboard with system metrics

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/new-feature`
3. Make your changes
4. Test thoroughly: `make up && make health`
5. Commit your changes: `git commit -m "Add new feature"`
6. Push to the branch: `git push origin feature/new-feature`
7. Submit a pull request

### Development Guidelines

- Follow PEP8 style guidelines for Python code
- Add comments in English
- Update documentation for new features
- Test all changes before submitting

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

For issues and questions:

1. Check the [Troubleshooting](#troubleshooting) section
2. Review service logs: `make logs`
3. Check service health: `make health`
4. Create an issue in the repository

## 🏷️ Version Information

- **Docker Compose**: 3.8
- **PostgreSQL**: 16-alpine
- **MongoDB**: 7
- **Apache Kafka**: Latest (Bitnami)
- **Apache Airflow**: 2.9.3
- **Apache Spark**: 3.5
- **Streamlit**: 1.29.0

---

**Built with ❤️ for Banesco Data Engineering Team**
