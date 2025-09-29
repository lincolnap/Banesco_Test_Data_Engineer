# Setup Parameters Directory

Este directorio contiene todos los scripts de configuración automática para el stack de Banesco Data Engineering.

## Archivos Incluidos

### Scripts de Configuración
- **`setup_airflow_variables.py`** - Configura las variables de Airflow necesarias para el pipeline
- **`setup_spark_connection.py`** - Configura la conexión Spark (standalone cluster)
- **`setup_postgres_connection.py`** - Configura la conexión PostgreSQL

### Scripts de Orquestación
- **`setup_complete.py`** - Script principal que ejecuta todos los scripts de configuración
- **`entrypoint.sh`** - Script de entrada que se ejecuta automáticamente al iniciar Airflow

## Uso

Estos scripts se ejecutan automáticamente cuando se inicia Airflow gracias al `entrypoint.sh` que está configurado en el Dockerfile.

### Ejecución Manual
Si necesitas ejecutar los scripts manualmente:

```bash
# Desde el contenedor de Airflow
python3 /opt/airflow/setup_parameters/setup_airflow_variables.py
python3 /opt/airflow/setup_parameters/setup_spark_connection.py
python3 /opt/airflow/setup_parameters/setup_postgres_connection.py

# O ejecutar todos de una vez
python3 /opt/airflow/setup_parameters/setup_complete.py
```

### Desde el Makefile
```bash
make setup-vars              # Solo variables
make setup-spark-connection  # Solo conexión Spark
make setup-postgres-connection # Solo conexión PostgreSQL
```

## Configuración

Los scripts están configurados para:
- **Spark**: Modo standalone (`spark://spark-master:7077`)
- **PostgreSQL**: Host `postgres`, puerto `5432`, base de datos `banesco_test`
- **MinIO**: Host `minio`, puerto `9000`

## Notas

- Los scripts esperan que Airflow esté ejecutándose en `http://localhost:8080`
- Las credenciales por defecto son `admin/admin` para Airflow
- Los scripts incluyen manejo de errores y reintentos automáticos
