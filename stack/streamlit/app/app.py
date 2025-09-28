import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import requests
import json

# Page configuration
st.set_page_config(
    page_title="Banesco Data Engineering Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .service-status {
        padding: 0.5rem;
        border-radius: 0.25rem;
        margin: 0.25rem 0;
    }
    .status-running {
        background-color: #d4edda;
        color: #155724;
        border-left: 4px solid #28a745;
    }
    .status-stopped {
        background-color: #f8d7da;
        color: #721c24;
        border-left: 4px solid #dc3545;
    }
</style>
""", unsafe_allow_html=True)

# Header
st.markdown('<h1 class="main-header">🏦 Banesco Data Engineering Stack</h1>', unsafe_allow_html=True)

# Sidebar
st.sidebar.title("📋 Navigation")
page = st.sidebar.selectbox(
    "Choose a page:",
    ["Dashboard", "Service Status", "Data Connectors", "Monitoring"]
)

# Service configurations
services = {
    "PostgreSQL": {"port": 5432, "url": "http://localhost:5432"},
    "MongoDB": {"port": 27017, "url": "http://localhost:27017"},
    "Kafka": {"port": 9092, "url": "http://localhost:9092"},
    "MinIO": {"port": 9000, "url": "http://localhost:9000"},
    "Airflow": {"port": 8080, "url": "http://localhost:8080"},
    "Spark Master": {"port": 8081, "url": "http://localhost:8081"},
    "Streamlit": {"port": 8501, "url": "http://localhost:8501"}
}

def check_service_status(service_name, port):
    """Check if a service is running by attempting to connect to its port"""
    try:
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        result = sock.connect_ex(('localhost', port))
        sock.close()
        return result == 0
    except:
        return False

if page == "Dashboard":
    st.header("📊 System Overview")
    
    # Metrics row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Active Services",
            value=len([s for s in services if check_service_status(s, services[s]["port"])]),
            delta=f"of {len(services)} total"
        )
    
    with col2:
        st.metric(
            label="Data Sources",
            value="3",
            delta="Postgres, MongoDB, Kafka"
        )
    
    with col3:
        st.metric(
            label="Processing Engines",
            value="2",
            delta="Spark, Airflow"
        )
    
    with col4:
        st.metric(
            label="Storage Systems",
            value="3",
            delta="Postgres, MongoDB, MinIO"
        )
    
    # Sample data visualization
    st.header("📈 Sample Analytics")
    
    # Generate sample data
    dates = pd.date_range(start='2024-01-01', end='2024-01-31', freq='D')
    sample_data = pd.DataFrame({
        'date': dates,
        'transactions': np.random.poisson(1000, len(dates)),
        'users': np.random.poisson(500, len(dates)),
        'revenue': np.random.normal(10000, 2000, len(dates))
    })
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Daily Transactions")
        fig = px.line(sample_data, x='date', y='transactions', 
                     title="Transaction Volume Over Time")
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Revenue Distribution")
        fig = px.histogram(sample_data, x='revenue', 
                          title="Revenue Distribution",
                          nbins=20)
        st.plotly_chart(fig, use_container_width=True)
    
    # Real-time metrics simulation
    st.header("⚡ Real-time Metrics")
    
    # Create placeholder for real-time data
    placeholder = st.empty()
    
    for i in range(5):
        with placeholder.container():
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("CPU Usage", f"{np.random.randint(20, 80)}%")
            
            with col2:
                st.metric("Memory Usage", f"{np.random.randint(40, 90)}%")
            
            with col3:
                st.metric("Active Connections", np.random.randint(10, 100))
            
        st.empty()  # Clear the container

elif page == "Service Status":
    st.header("🔍 Service Status Monitor")
    
    for service_name, config in services.items():
        col1, col2 = st.columns([3, 1])
        
        with col1:
            status = "🟢 Running" if check_service_status(service_name, config["port"]) else "🔴 Stopped"
            st.write(f"**{service_name}** - {status}")
            st.write(f"Port: {config['port']} | URL: {config['url']}")
        
        with col2:
            if check_service_status(service_name, config["port"]):
                st.markdown('<div class="service-status status-running">Online</div>', unsafe_allow_html=True)
            else:
                st.markdown('<div class="service-status status-stopped">Offline</div>', unsafe_allow_html=True)

elif page == "Data Connectors":
    st.header("🔗 Data Connectors")
    
    # Connection examples
    st.subheader("Database Connections")
    
    with st.expander("PostgreSQL Connection"):
        st.code("""
import psycopg2
conn = psycopg2.connect(
    host="postgres",
    port=5432,
    database="airflow_db",
    user="postgres",
    password="postgres123"
)
        """, language="python")
    
    with st.expander("MongoDB Connection"):
        st.code("""
from pymongo import MongoClient
client = MongoClient(
    host="mongodb",
    port=27017,
    username="admin",
    password="admin123"
)
        """, language="python")
    
    with st.expander("Kafka Producer"):
        st.code("""
from kafka import KafkaProducer
producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)
        """, language="python")
    
    with st.expander("MinIO Client"):
        st.code("""
from minio import Minio
client = Minio(
    'minio:9000',
    access_key='minioadmin',
    secret_key='minioadmin123',
    secure=False
)
        """, language="python")

elif page == "Monitoring":
    st.header("📊 System Monitoring")
    
    # System metrics simulation
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Resource Usage")
        
        # CPU usage
        cpu_data = pd.DataFrame({
            'time': pd.date_range(start='2024-01-01', periods=24, freq='H'),
            'cpu': np.random.normal(50, 15, 24)
        })
        
        fig = px.area(cpu_data, x='time', y='cpu', 
                     title="CPU Usage Over Time")
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Memory Usage")
        
        # Memory usage
        memory_data = pd.DataFrame({
            'time': pd.date_range(start='2024-01-01', periods=24, freq='H'),
            'memory': np.random.normal(60, 10, 24)
        })
        
        fig = px.area(memory_data, x='time', y='memory', 
                     title="Memory Usage Over Time")
        st.plotly_chart(fig, use_container_width=True)
    
    # Alerts section
    st.subheader("🚨 System Alerts")
    
    alerts = [
        {"service": "Kafka", "level": "Warning", "message": "High message lag detected", "time": "2024-01-15 14:30"},
        {"service": "PostgreSQL", "level": "Info", "message": "Backup completed successfully", "time": "2024-01-15 14:00"},
        {"service": "Airflow", "level": "Error", "message": "DAG execution failed", "time": "2024-01-15 13:45"},
    ]
    
    for alert in alerts:
        if alert["level"] == "Error":
            st.error(f"🔴 **{alert['service']}**: {alert['message']} - {alert['time']}")
        elif alert["level"] == "Warning":
            st.warning(f"🟡 **{alert['service']}**: {alert['message']} - {alert['time']}")
        else:
            st.info(f"🔵 **{alert['service']}**: {alert['message']} - {alert['time']}")

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666;'>
    <p>Banesco Data Engineering Stack - Built with Streamlit 🚀</p>
    <p>Last updated: {}</p>
</div>
""".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")), unsafe_allow_html=True)
