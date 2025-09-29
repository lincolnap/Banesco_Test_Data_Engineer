"""
Database Connector for Streamlit Dashboard
Handles connections to PostgreSQL for Divvy Bikes analytics
"""

import os
import psycopg2
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine
import logging

logger = logging.getLogger(__name__)

class DatabaseConnector:
    """Database connector for PostgreSQL analytics database"""
    
    def __init__(self):
        self.config = {
            "host": os.getenv("POSTGRES_HOST", "postgres"),  # Use Docker service name
            "port": os.getenv("POSTGRES_PORT", "5432"),      # Internal Docker port
            "database": os.getenv("POSTGRES_DB", "banesco_test"),
            "user": os.getenv("POSTGRES_USER", "postgres"),
            "password": os.getenv("POSTGRES_PASSWORD", "postgres123")
        }
        self.connection_string = f"postgresql://{self.config['user']}:{self.config['password']}@{self.config['host']}:{self.config['port']}/{self.config['database']}"
    
    @st.cache_resource
    def get_connection(_self):
        """Get cached database connection"""
        try:
            engine = create_engine(_self.connection_string)
            return engine
        except Exception as e:
            st.error(f"Error connecting to database: {str(e)}")
            return None
    
    @st.cache_data(ttl=int(os.getenv("STREAMLIT_AUTO_REFRESH_INTERVAL", "30")))  # Configurable auto-refresh
    def execute_query(_self, query: str, params=None):
        """Execute SQL query and return DataFrame"""
        try:
            engine = _self.get_connection()
            if engine is None:
                return pd.DataFrame()
            
            df = pd.read_sql_query(query, engine, params=params)
            return df
        except Exception as e:
            st.error(f"Error executing query: {str(e)}")
            return pd.DataFrame()
    
    def get_member_distance_kpi(self):
        """Get member distance KPIs"""
        query = """
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
        """
        return self.execute_query(query)
    
    def get_daily_trends(self, days=30):
        """Get daily trends for the last N days"""
        query = """
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
        """
        return self.execute_query(query, params=(days,))
    
    def get_station_popularity(self, limit=20):
        """Get top stations by popularity"""
        query = """
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
        """
        return self.execute_query(query, params=(limit,))
    
    def get_hourly_distribution(self):
        """Get hourly ride distribution"""
        query = """
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
        """
        return self.execute_query(query)
    
    def get_distance_category_analysis(self):
        """Get distance category analysis"""
        query = """
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
        """
        return self.execute_query(query)
    
    def get_time_of_day_analysis(self):
        """Get time of day analysis"""
        query = """
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
        """
        return self.execute_query(query)
    
    def get_weekly_patterns(self):
        """Get weekly patterns"""
        query = """
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
        """
        return self.execute_query(query)
    
    def get_top_routes(self, limit=10):
        """Get top routes by frequency"""
        query = """
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
        """
        return self.execute_query(query, params=(limit,))
    
    def get_database_stats(self):
        """Get database statistics"""
        query = """
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
        return self.execute_query(query)
    
    def get_quarter_analysis(self, quarter):
        """Get quarter analysis for specified quarter"""
        query = """
        SELECT 
            COUNT(*) as total_rides,
            ROUND(SUM(total_distance), 2) as total_distance,
            ROUND(AVG(total_distance), 2) as avg_distance,
            ROUND(AVG(ride_duration_minutes), 2) as avg_duration
        FROM analytics.fact_rides 
        WHERE EXTRACT(QUARTER FROM dt) = %s;
        """
        return self.execute_query(query, params=(quarter,))
    
    def get_quarter_by_member_type(self, quarter):
        """Get quarter analysis by member type for specified quarter"""
        query = """
        SELECT 
            member_casual,
            COUNT(*) as total_rides,
            ROUND(SUM(total_distance), 2) as total_distance,
            ROUND(AVG(total_distance), 2) as avg_distance
        FROM analytics.fact_rides 
        WHERE EXTRACT(QUARTER FROM dt) = %s
        GROUP BY member_casual
        ORDER BY member_casual;
        """
        return self.execute_query(query, params=(quarter,))
    
    def get_quarter_monthly_trend(self, quarter):
        """Get monthly trend within specified quarter"""
        query = """
        SELECT 
            EXTRACT(MONTH FROM dt) as month,
            CASE EXTRACT(MONTH FROM dt)
                WHEN 1 THEN 'January'
                WHEN 2 THEN 'February'
                WHEN 3 THEN 'March'
                WHEN 4 THEN 'April'
                WHEN 5 THEN 'May'
                WHEN 6 THEN 'June'
                WHEN 7 THEN 'July'
                WHEN 8 THEN 'August'
                WHEN 9 THEN 'September'
                WHEN 10 THEN 'October'
                WHEN 11 THEN 'November'
                WHEN 12 THEN 'December'
            END as month_name,
            COUNT(*) as total_rides,
            ROUND(SUM(total_distance), 2) as total_distance
        FROM analytics.fact_rides 
        WHERE EXTRACT(QUARTER FROM dt) = %s
        GROUP BY EXTRACT(MONTH FROM dt)
        ORDER BY month;
        """
        return self.execute_query(query, params=(quarter,))
    
    def get_daily_comparison(self):
        """Get daily comparison for last 30 days"""
        query = """
        SELECT 
            dt as period,
            TO_CHAR(dt, 'YYYY-MM-DD') as period_label,
            COUNT(*) as total_rides,
            ROUND(SUM(total_distance), 2) as total_distance
        FROM analytics.fact_rides 
        WHERE dt >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY dt
        ORDER BY dt;
        """
        return self.execute_query(query)
    
    def get_monthly_comparison(self):
        """Get monthly comparison"""
        query = """
        SELECT 
            DATE_TRUNC('month', dt) as period,
            TO_CHAR(dt, 'YYYY-MM') as period_label,
            COUNT(*) as total_rides,
            ROUND(SUM(total_distance), 2) as total_distance
        FROM analytics.fact_rides 
        GROUP BY DATE_TRUNC('month', dt), TO_CHAR(dt, 'YYYY-MM')
        ORDER BY period;
        """
        return self.execute_query(query)
    
    def get_quarterly_comparison(self):
        """Get quarterly comparison"""
        query = """
        SELECT 
            EXTRACT(YEAR FROM dt) as year,
            EXTRACT(QUARTER FROM dt) as quarter,
            EXTRACT(YEAR FROM dt) || '-Q' || EXTRACT(QUARTER FROM dt) as period_label,
            COUNT(*) as total_rides,
            ROUND(SUM(total_distance), 2) as total_distance
        FROM analytics.fact_rides 
        GROUP BY EXTRACT(YEAR FROM dt), EXTRACT(QUARTER FROM dt)
        ORDER BY year, quarter;
        """
        return self.execute_query(query)
    
    def get_yearly_comparison(self):
        """Get yearly comparison"""
        query = """
        SELECT 
            EXTRACT(YEAR FROM dt) as year,
            EXTRACT(YEAR FROM dt)::text as period_label,
            COUNT(*) as total_rides,
            ROUND(SUM(total_distance), 2) as total_distance
        FROM analytics.fact_rides 
        GROUP BY EXTRACT(YEAR FROM dt)
        ORDER BY year;
        """
        return self.execute_query(query)
