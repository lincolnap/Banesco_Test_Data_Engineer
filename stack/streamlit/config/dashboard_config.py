"""
Dashboard Configuration
Configuration settings for the Divvy Bikes Analytics Dashboard
"""

# Database Configuration
DATABASE_CONFIG = {
    "host": "postgres",
    "port": "5433",
    "database": "banesco_test",
    "user": "postgres",
    "password": "postgres123",
    "schema": "analytics"
}

# Chart Configuration
CHART_CONFIG = {
    "colors": {
        "member": "#1f77b4",
        "casual": "#ff7f0e",
        "primary": "#2E86AB",
        "secondary": "#A23B72",
        "success": "#F18F01",
        "warning": "#C73E1D"
    },
    "default_height": 400,
    "kpi_height": 600
}

# Analysis Configuration
ANALYSIS_CONFIG = {
    "default_days": 30,
    "top_stations_limit": 20,
    "top_routes_limit": 10,
    "cache_ttl": 300  # 5 minutes
}

# KPI Configuration
KPI_CONFIG = {
    "distance_categories": {
        "SHORT": {"min": 0, "max": 1, "color": "#2E86AB"},
        "MEDIUM": {"min": 1, "max": 5, "color": "#A23B72"},
        "LONG": {"min": 5, "max": 15, "color": "#F18F01"},
        "VERY_LONG": {"min": 15, "max": float('inf'), "color": "#C73E1D"}
    },
    "duration_categories": {
        "QUICK": {"min": 0, "max": 15, "color": "#2E86AB"},
        "NORMAL": {"min": 15, "max": 60, "color": "#A23B72"},
        "EXTENDED": {"min": 60, "max": 180, "color": "#F18F01"},
        "VERY_EXTENDED": {"min": 180, "max": float('inf'), "color": "#C73E1D"}
    },
    "time_periods": {
        "MORNING": {"start": 6, "end": 11, "color": "#FFD700"},
        "AFTERNOON": {"start": 12, "end": 17, "color": "#FFA500"},
        "EVENING": {"start": 18, "end": 22, "color": "#FF6347"},
        "NIGHT": {"start": 23, "end": 5, "color": "#4169E1"}
    }
}

# Dashboard Pages Configuration
PAGES_CONFIG = {
    "main_pages": [
        {"name": "🏠 Dashboard", "description": "System overview and monitoring"},
        {"name": "🚴 Divvy Bikes Analytics", "description": "Comprehensive bike analytics"},
        {"name": "📊 Member Distance Analysis", "description": "Deep dive into member distance KPIs"},
        {"name": "🔍 Service Status", "description": "Service health monitoring"},
        {"name": "🔗 Data Connectors", "description": "Database connection examples"},
        {"name": "📈 Monitoring", "description": "System performance monitoring"}
    ],
    "analytics_tabs": [
        {"name": "📊 Overview", "description": "General analytics and KPIs"},
        {"name": "⏰ Temporal Analysis", "description": "Time-based analysis"},
        {"name": "🗺️ Geographic Analysis", "description": "Location and route analysis"},
        {"name": "👥 User Behavior", "description": "User behavior patterns"}
    ]
}

# SQL Queries Configuration
SQL_QUERIES = {
    "member_distance_kpi": """
        SELECT 
            member_casual,
            total_rides,
            avg_distance_km,
            total_distance_km,
            median_distance_km,
            p75_distance_km,
            p25_distance_km,
            avg_duration_minutes,
            unique_start_stations,
            unique_end_stations
        FROM analytics.v_member_distance_kpi
        ORDER BY member_casual;
    """,
    "daily_trends": """
        SELECT 
            dt,
            member_casual,
            daily_rides,
            avg_daily_distance,
            total_daily_distance,
            avg_daily_duration
        FROM analytics.v_daily_trends
        WHERE dt >= CURRENT_DATE - INTERVAL '%s days'
        ORDER BY dt DESC, member_casual;
    """,
    "station_popularity": """
        SELECT 
            station_name,
            station_id,
            total_rides,
            active_days,
            avg_distance,
            avg_duration,
            member_rides,
            casual_rides
        FROM analytics.v_station_popularity
        ORDER BY total_rides DESC
        LIMIT %s;
    """,
    "hourly_distribution": """
        SELECT 
            hour_of_day,
            member_casual,
            COUNT(*) as ride_count,
            AVG(total_distance) as avg_distance,
            AVG(ride_duration_minutes) as avg_duration
        FROM analytics.fact_rides
        WHERE total_distance > 0 AND ride_duration_minutes > 0
        GROUP BY hour_of_day, member_casual
        ORDER BY hour_of_day, member_casual;
    """,
    "distance_category_analysis": """
        SELECT 
            distance_category,
            member_casual,
            COUNT(*) as ride_count,
            AVG(total_distance) as avg_distance,
            AVG(ride_duration_minutes) as avg_duration,
            SUM(total_distance) as total_distance
        FROM analytics.fact_rides
        WHERE total_distance > 0 AND ride_duration_minutes > 0
        GROUP BY distance_category, member_casual
        ORDER BY 
            CASE distance_category 
                WHEN 'SHORT' THEN 1
                WHEN 'MEDIUM' THEN 2
                WHEN 'LONG' THEN 3
                WHEN 'VERY_LONG' THEN 4
            END,
            member_casual;
    """,
    "time_of_day_analysis": """
        SELECT 
            time_of_day,
            member_casual,
            COUNT(*) as ride_count,
            AVG(total_distance) as avg_distance,
            AVG(ride_duration_minutes) as avg_duration
        FROM analytics.fact_rides
        WHERE total_distance > 0 AND ride_duration_minutes > 0
        GROUP BY time_of_day, member_casual
        ORDER BY 
            CASE time_of_day 
                WHEN 'MORNING' THEN 1
                WHEN 'AFTERNOON' THEN 2
                WHEN 'EVENING' THEN 3
                WHEN 'NIGHT' THEN 4
            END,
            member_casual;
    """,
    "weekly_patterns": """
        SELECT 
            day_of_week,
            member_casual,
            COUNT(*) as ride_count,
            AVG(total_distance) as avg_distance,
            AVG(ride_duration_minutes) as avg_duration
        FROM analytics.fact_rides
        WHERE total_distance > 0 AND ride_duration_minutes > 0
        GROUP BY day_of_week, member_casual
        ORDER BY day_of_week, member_casual;
    """,
    "top_routes": """
        SELECT 
            start_station_name,
            end_station_name,
            COUNT(*) as ride_count,
            AVG(total_distance) as avg_distance,
            AVG(ride_duration_minutes) as avg_duration,
            SUM(CASE WHEN member_casual = 'MEMBER' THEN 1 ELSE 0 END) as member_rides,
            SUM(CASE WHEN member_casual = 'CASUAL' THEN 1 ELSE 0 END) as casual_rides
        FROM analytics.fact_rides
        WHERE start_station_name IS NOT NULL 
            AND end_station_name IS NOT NULL
            AND start_station_name != end_station_name
            AND total_distance > 0
        GROUP BY start_station_name, end_station_name
        ORDER BY ride_count DESC
        LIMIT %s;
    """,
    "database_stats": """
        SELECT 
            'Total Rides' as metric,
            COUNT(*)::text as value
        FROM analytics.fact_rides
        UNION ALL
        SELECT 
            'Total Distance (km)' as metric,
            ROUND(SUM(total_distance), 2)::text as value
        FROM analytics.fact_rides
        UNION ALL
        SELECT 
            'Unique Stations' as metric,
            COUNT(DISTINCT start_station_id)::text as value
        FROM analytics.fact_rides
        UNION ALL
        SELECT 
            'Date Range' as metric,
            CONCAT(MIN(dt)::text, ' to ', MAX(dt)::text) as value
        FROM analytics.fact_rides;
    """
}
