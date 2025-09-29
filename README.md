# 🚀 Banesco Data Engineering Stack

Sistema de procesamiento de datos para análisis de bicicletas Divvy con Apache Airflow, Spark, PostgreSQL y MinIO.

## 📋 Tabla de Contenidos

- [Requisitos Previos](#-requisitos-previos)
- [Configuración del Entorno](#-configuración-del-entorno)
- [Levantar el Stack](#-levantar-el-stack)
- [Ejecutar la Solución](#-ejecutar-la-solución)
- [Servicios Disponibles](#-servicios-disponibles)
- [Solución de Problemas](#-solución-de-problemas)

## 🔧 Requisitos Previos

Antes de comenzar, asegúrate de tener instalado:

- **Docker** (versión 20.10 o superior)
- **Docker Compose** (versión 2.0 o superior)
- **Make** (para usar los comandos del Makefile)
- **Git** (para clonar el repositorio)

### Verificar Instalación

```bash
# Verificar Docker
docker --version
docker-compose --version

# Verificar Make
make --version
```

## ⚙️ Configuración del Entorno

### 1. Clonar el Repositorio

```bash
git clone <url-del-repositorio>
cd Banesco_Test_Data_Engineer
```

### 2. Verificar Docker

Asegúrate de que Docker esté ejecutándose:

```bash
docker info
```

Si no está ejecutándose, inicia Docker Desktop.

### 3. Configurar Variables de Entorno (Opcional)

El sistema funciona con valores por defecto, pero puedes personalizar:

```bash
# Crear archivo .env (opcional)
cp .env.example .env
# Editar las variables según necesites
```

## 🚀 Levantar el Stack

### Opción 1: Stack Completo (Recomendado)

```bash
make start
```

Este comando:
- ✅ Levanta todos los servicios (PostgreSQL, MinIO, Spark, Airflow, Streamlit)
- ✅ Configura automáticamente las variables y conexiones
- ✅ Crea los buckets necesarios en MinIO
- ✅ Inicia el dashboard de Streamlit

### Opción 2: Stack Básico

```bash
make start-stack
```

Solo levanta los servicios sin configuración automática.

### Verificar que Todo Funciona

```bash
# Ver estado de todos los contenedores
make status

# Ver logs si hay problemas
make logs
```

## 🎯 Ejecutar la Solución

### 1. Acceder a Airflow

1. Abre tu navegador en: **http://localhost:8080**
2. Usuario: `admin`
3. Contraseña: `admin`

### 2. Ejecutar el Pipeline

1. **Encontrar el DAG**: Busca `data_bike_pipeline` en la lista de DAGs
2. **Habilitar el DAG**: Haz clic en el toggle para activarlo
3. **Ejecutar**: Haz clic en el botón "Trigger DAG" (▶️)
4. **Monitorear**: Ve el progreso en tiempo real

### 3. Verificar los Resultados

El pipeline procesa datos de bicicletas Divvy y los guarda en:
- **MinIO**: Datos en formato Parquet
- **PostgreSQL**: Datos procesados para análisis

## 🌐 Servicios Disponibles

| Servicio | URL | Usuario/Contraseña | Descripción |
|----------|-----|-------------------|-------------|
| **Airflow** | http://localhost:8080 | admin/admin | Orquestación de tareas |
| **Streamlit** | http://localhost:8501 | - | Dashboard de visualización |
| **Spark Master** | http://localhost:8081 | - | Interfaz de Spark |
| **MinIO** | http://localhost:9001 | minioadmin/minioadmin123 | Almacenamiento de objetos |
| **PostgreSQL** | localhost:5433 | postgres/postgres123 | Base de datos |

### Acceso Rápido

```bash
# Abrir todos los servicios
make urls
```

## 🔍 Monitoreo y Logs

### Ver Logs de un Servicio Específico

```bash
# Logs de Airflow
docker logs banesco_airflow_scheduler

# Logs de Spark
docker logs banesco_spark_master

# Logs de MinIO
docker logs banesco_minio
```

### Ver Estado del Sistema

```bash
# Estado de todos los contenedores
make status

# Logs en tiempo real
make logs
```

## 🛠️ Comandos Útiles

### Gestión del Stack

```bash
# Iniciar todo
make start

# Parar todo
make stop

# Reiniciar todo
make restart

# Ver estado
make status
```

### Desarrollo

```bash
# Construir imagen de Airflow
make build

# Limpiar contenedores
make clean

# Limpiar TODO (incluyendo datos)
make clean-all
```

### Configuración

```bash
# Configurar variables de Airflow
make setup-vars

# Configurar conexión PostgreSQL
make setup-postgres-connection

# Configurar conexión Spark
make setup-spark-connection
```

## 🚨 Solución de Problemas

### Problema: "Docker no está ejecutándose"

**Solución:**
```bash
# Iniciar Docker Desktop
# O en Linux:
sudo systemctl start docker
```

### Problema: "Puerto ya en uso"

**Solución:**
```bash
# Ver qué está usando el puerto
lsof -i :8080

# Parar el proceso o cambiar puerto en docker-compose.yml
```

### Problema: "Airflow no responde"

**Solución:**
```bash
# Reiniciar Airflow
docker-compose restart airflow-scheduler airflow-webserver

# Verificar logs
docker logs banesco_airflow_scheduler
```

### Problema: "Error de conexión a base de datos"

**Solución:**
```bash
# Reconfigurar conexiones
make setup-postgres-connection

# Verificar que PostgreSQL esté corriendo
docker ps | grep postgres
```

### Problema: "Spark no funciona"

**Solución:**
```bash
# Verificar que Spark esté corriendo
docker ps | grep spark

# Reiniciar Spark
docker-compose restart spark-master spark-worker
```

## 📊 Flujo de Datos

```
1. 📥 Extracción: Datos de Divvy Bikes → MinIO (Raw Zone)
2. 🔄 Transformación: Procesamiento con Spark → MinIO (Stage Zone)  
3. 🗄️ Carga: Datos finales → PostgreSQL (Analytics)
4. 📊 Visualización: Dashboard en Streamlit
```

## 🆘 Obtener Ayuda

Si tienes problemas:

1. **Revisa los logs**: `make logs`
2. **Verifica el estado**: `make status`
3. **Consulta la documentación técnica**: `README_INFRASTRUCTURE.md`
4. **Reinicia el stack**: `make restart`

## 🎉 ¡Listo!

Tu stack de Data Engineering está funcionando. Ahora puedes:

- ✅ Ejecutar pipelines de datos en Airflow
- ✅ Visualizar datos en Streamlit
- ✅ Monitorear el procesamiento en Spark
- ✅ Gestionar archivos en MinIO
- ✅ Consultar datos en PostgreSQL

---

**¿Necesitas más detalles técnicos?** Consulta `README_INFRASTRUCTURE.md` para información avanzada sobre la arquitectura y configuración.
