import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import requests
import json
import sys
import os

# Add utils to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))

from database_connector import DatabaseConnector
from chart_helpers import (
    create_member_distance_kpi_chart,
    create_daily_trends_chart,
    create_hourly_distribution_chart,
    create_distance_category_chart,
    create_time_of_day_chart,
    create_weekly_patterns_chart,
    create_station_popularity_chart,
    create_top_routes_chart,
    create_database_stats_cards
)

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
    ["🏠 Dashboard", "🚴 Divvy Bikes Analytics", "📊 Member Distance Analysis", "🔍 Service Status", "🔗 Data Connectors", "📈 Monitoring"]
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

if page == "🏠 Dashboard":
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

elif page == "🚴 Divvy Bikes Analytics":
    st.header("🚴 Divvy Bikes Analytics Dashboard")
    
    # Initialize database connector
    db_connector = DatabaseConnector()
    
    # Database connection status
    try:
        stats_df = db_connector.get_database_stats()
        if not stats_df.empty:
            st.success("✅ Connected to PostgreSQL Analytics Database")
            
            # Display key metrics
            col1, col2, col3, col4 = st.columns(4)
            
            stats = create_database_stats_cards(stats_df)
            
            with col1:
                st.metric("Total Rides", stats.get('Total Rides', 'N/A'))
            with col2:
                st.metric("Total Distance", f"{stats.get('Total Distance (km)', 'N/A')} km")
            with col3:
                st.metric("Unique Stations", stats.get('Unique Stations', 'N/A'))
            with col4:
                st.metric("Date Range", stats.get('Date Range', 'N/A')[:20] + "..." if len(stats.get('Date Range', '')) > 20 else stats.get('Date Range', 'N/A'))
        else:
            st.warning("⚠️ No data available in analytics database")
    except Exception as e:
        st.error(f"❌ Error connecting to database: {str(e)}")
        st.stop()
    
    # Main analytics sections
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "⏰ Temporal Analysis", "🗺️ Geographic Analysis", "👥 User Behavior"])
    
    with tab1:
        st.subheader("📊 Overview Analytics")
        
        # Member Distance KPIs
        st.subheader("🎯 Member Distance Analysis - Key Performance Indicators")
        member_kpi_df = db_connector.get_member_distance_kpi()
        
        if not member_kpi_df.empty:
            # Display KPI chart
            fig_kpi = create_member_distance_kpi_chart(member_kpi_df)
            st.plotly_chart(fig_kpi, use_container_width=True)
            
            # Display KPI table
            st.subheader("📋 Detailed KPI Metrics")
            st.dataframe(member_kpi_df, use_container_width=True)
        else:
            st.warning("No member distance KPI data available")
        
        # Station Popularity
        st.subheader("🏆 Top Popular Stations")
        station_df = db_connector.get_station_popularity()
        
        if not station_df.empty:
            fig_stations = create_station_popularity_chart(station_df)
            st.plotly_chart(fig_stations, use_container_width=True)
        else:
            st.warning("No station data available")
    
    with tab2:
        st.subheader("⏰ Temporal Analysis")
        
        # Daily trends
        st.subheader("📈 Daily Distance Trends")
        daily_df = db_connector.get_daily_trends(days=30)
        
        if not daily_df.empty:
            fig_daily = create_daily_trends_chart(daily_df)
            st.plotly_chart(fig_daily, use_container_width=True)
        else:
            st.warning("No daily trends data available")
        
        # Hourly distribution
        st.subheader("🕐 Hourly Ride Distribution")
        hourly_df = db_connector.get_hourly_distribution()
        
        if not hourly_df.empty:
            fig_hourly = create_hourly_distribution_chart(hourly_df)
            st.plotly_chart(fig_hourly, use_container_width=True)
        else:
            st.warning("No hourly data available")
        
        # Weekly patterns
        st.subheader("📅 Weekly Patterns")
        weekly_df = db_connector.get_weekly_patterns()
        
        if not weekly_df.empty:
            fig_weekly = create_weekly_patterns_chart(weekly_df)
            st.plotly_chart(fig_weekly, use_container_width=True)
        else:
            st.warning("No weekly data available")
    
    with tab3:
        st.subheader("🗺️ Geographic Analysis")
        
        # Top routes
        st.subheader("🛣️ Top Popular Routes")
        routes_df = db_connector.get_top_routes()
        
        if not routes_df.empty:
            fig_routes = create_top_routes_chart(routes_df)
            st.plotly_chart(fig_routes, use_container_width=True)
        else:
            st.warning("No routes data available")
        
        # Distance categories
        st.subheader("📏 Distance Category Analysis")
        distance_df = db_connector.get_distance_category_analysis()
        
        if not distance_df.empty:
            fig_distance = create_distance_category_chart(distance_df)
            st.plotly_chart(fig_distance, use_container_width=True)
        else:
            st.warning("No distance category data available")
    
    with tab4:
        st.subheader("👥 User Behavior Analysis")
        
        # Time of day analysis
        st.subheader("🌅 Time of Day Preferences")
        time_df = db_connector.get_time_of_day_analysis()
        
        if not time_df.empty:
            fig_time = create_time_of_day_chart(time_df)
            st.plotly_chart(fig_time, use_container_width=True)
        else:
            st.warning("No time of day data available")

elif page == "📊 Member Distance Analysis":
    st.header("📊 Member Distance Analysis - Deep Dive")
    
    # Initialize database connector
    db_connector = DatabaseConnector()
    
    # Get member distance KPI data
    member_kpi_df = db_connector.get_member_distance_kpi()
    
    if member_kpi_df.empty:
        st.warning("⚠️ No member distance data available. Please ensure data has been loaded to PostgreSQL.")
        st.stop()
    
    # Main KPI Cards
    st.subheader("🎯 Key Performance Indicators")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        member_data = member_kpi_df[member_kpi_df['member_casual'] == 'MEMBER']
        casual_data = member_kpi_df[member_kpi_df['member_casual'] == 'CASUAL']
        
        if not member_data.empty and not casual_data.empty:
            member_avg = member_data['avg_distance_km'].iloc[0]
            casual_avg = casual_data['avg_distance_km'].iloc[0]
            diff = member_avg - casual_avg
            
            st.metric(
                "Member Avg Distance", 
                f"{member_avg:.3f} km",
                delta=f"{diff:+.3f} km vs Casual"
            )
    
    with col2:
        if not member_data.empty and not casual_data.empty:
            member_total = member_data['total_distance_km'].iloc[0]
            casual_total = casual_data['total_distance_km'].iloc[0]
            
            st.metric(
                "Member Total Distance", 
                f"{member_total:.2f} km",
                delta=f"{((member_total - casual_total) / casual_total * 100):+.1f}% vs Casual"
            )
    
    with col3:
        if not member_data.empty and not casual_data.empty:
            member_median = member_data['median_distance_km'].iloc[0]
            casual_median = casual_data['median_distance_km'].iloc[0]
            
            st.metric(
                "Member Median Distance", 
                f"{member_median:.3f} km",
                delta=f"{member_median - casual_median:+.3f} km vs Casual"
            )
    
    with col4:
        if not member_data.empty and not casual_data.empty:
            member_duration = member_data['avg_duration_minutes'].iloc[0]
            casual_duration = casual_data['avg_duration_minutes'].iloc[0]
            
            st.metric(
                "Member Avg Duration", 
                f"{member_duration:.1f} min",
                delta=f"{member_duration - casual_duration:+.1f} min vs Casual"
            )
    
    # Detailed Analysis Charts
    st.subheader("📈 Detailed Analysis")
    
    # Create comprehensive KPI chart
    fig_kpi = create_member_distance_kpi_chart(member_kpi_df)
    st.plotly_chart(fig_kpi, use_container_width=True)
    
    # Comparative Analysis Table
    st.subheader("📋 Comparative Analysis Table")
    
    # Create comparison table
    comparison_df = member_kpi_df.pivot(
        index=['total_rides', 'avg_distance_km', 'total_distance_km', 'median_distance_km', 
               'p75_distance_km', 'p25_distance_km', 'avg_duration_minutes', 
               'unique_start_stations', 'unique_end_stations'],
        columns='member_casual',
        values='total_rides'
    ).fillna(0)
    
    # Display the detailed metrics
    st.dataframe(member_kpi_df, use_container_width=True)
    
    # Insights Section
    st.subheader("💡 Key Insights")
    
    if not member_data.empty and not casual_data.empty:
        insights = []
        
        # Distance insights
        if member_avg > casual_avg:
            insights.append(f"✅ **Members ride longer distances** on average ({member_avg:.3f} km vs {casual_avg:.3f} km)")
        else:
            insights.append(f"ℹ️ **Casual users ride longer distances** on average ({casual_avg:.3f} km vs {member_avg:.3f} km)")
        
        # Duration insights
        if member_duration > casual_duration:
            insights.append(f"⏱️ **Members have longer ride durations** on average ({member_duration:.1f} min vs {casual_duration:.1f} min)")
        else:
            insights.append(f"⏱️ **Casual users have longer ride durations** on average ({casual_duration:.1f} min vs {member_duration:.1f} min)")
        
        # Station usage insights
        member_stations = member_data['unique_start_stations'].iloc[0]
        casual_stations = casual_data['unique_start_stations'].iloc[0]
        
        if member_stations > casual_stations:
            insights.append(f"🚴 **Members use more diverse stations** ({member_stations} vs {casual_stations} unique stations)")
        else:
            insights.append(f"🚴 **Casual users use more diverse stations** ({casual_stations} vs {member_stations} unique stations)")
        
        # Display insights
        for insight in insights:
            st.markdown(insight)
    
    # Additional Analysis Options
    st.subheader("🔍 Additional Analysis")
    
    analysis_type = st.selectbox(
        "Select additional analysis:",
        ["Daily Trends", "Hourly Distribution", "Distance Categories", "Time of Day"]
    )
    
    if analysis_type == "Daily Trends":
        daily_df = db_connector.get_daily_trends(days=30)
        if not daily_df.empty:
            fig_daily = create_daily_trends_chart(daily_df)
            st.plotly_chart(fig_daily, use_container_width=True)
    
    elif analysis_type == "Hourly Distribution":
        hourly_df = db_connector.get_hourly_distribution()
        if not hourly_df.empty:
            fig_hourly = create_hourly_distribution_chart(hourly_df)
            st.plotly_chart(fig_hourly, use_container_width=True)
    
    elif analysis_type == "Distance Categories":
        distance_df = db_connector.get_distance_category_analysis()
        if not distance_df.empty:
            fig_distance = create_distance_category_chart(distance_df)
            st.plotly_chart(fig_distance, use_container_width=True)
    
    elif analysis_type == "Time of Day":
        time_df = db_connector.get_time_of_day_analysis()
        if not time_df.empty:
            fig_time = create_time_of_day_chart(time_df)
            st.plotly_chart(fig_time, use_container_width=True)

elif page == "🔍 Service Status":
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

elif page == "🔗 Data Connectors":
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

elif page == "📈 Monitoring":
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
