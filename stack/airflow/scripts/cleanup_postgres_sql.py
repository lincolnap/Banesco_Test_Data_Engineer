#!/usr/bin/env python3
"""
Generate SQL cleanup commands for PostgreSQL based on year-month
This script generates SQL that will be executed by PostgresOperator
"""

import sys
from datetime import datetime, timedelta

def get_date_range_from_year_month(year_month):
    """Convert YYYYMM format to start and end dates"""
    try:
        year = int(year_month[:4])
        month = int(year_month[4:6])
        
        start_date = datetime(year, month, 1).date()
        
        # Calculate end date (last day of the month)
        if month == 12:
            end_date = datetime(year + 1, 1, 1).date() - timedelta(days=1)
        else:
            end_date = datetime(year, month + 1, 1).date() - timedelta(days=1)
            
        return start_date, end_date
        
    except Exception as e:
        print(f"Error parsing year_month {year_month}: {str(e)}")
        raise

def generate_cleanup_sql(year_month, cleanup_mode="date_range"):
    """Generate SQL cleanup commands"""
    
    start_date, end_date = get_date_range_from_year_month(year_month)
    
    if cleanup_mode == "truncate_all":
        sql = """
-- Truncate all analytics tables
TRUNCATE TABLE analytics.agg_user_behavior CASCADE;
TRUNCATE TABLE analytics.agg_station_metrics CASCADE;
TRUNCATE TABLE analytics.agg_geographical_analysis CASCADE;
TRUNCATE TABLE analytics.agg_daily_metrics CASCADE;
TRUNCATE TABLE analytics.fact_rides CASCADE;
TRUNCATE TABLE analytics.dim_stations CASCADE;
TRUNCATE TABLE analytics.dim_time CASCADE;

-- Analyze tables for performance
ANALYZE analytics.fact_rides;
ANALYZE analytics.dim_stations;
ANALYZE analytics.dim_time;
ANALYZE analytics.agg_daily_metrics;
"""
    
    elif cleanup_mode == "date_range":
        sql = f"""
-- Delete data for date range: {start_date} to {end_date}
DELETE FROM analytics.agg_user_behavior WHERE date_key BETWEEN '{start_date}' AND '{end_date}';
DELETE FROM analytics.agg_station_metrics WHERE date_key BETWEEN '{start_date}' AND '{end_date}';
DELETE FROM analytics.agg_geographical_analysis WHERE date_key BETWEEN '{start_date}' AND '{end_date}';
DELETE FROM analytics.agg_daily_metrics WHERE date_key BETWEEN '{start_date}' AND '{end_date}';
DELETE FROM analytics.fact_rides WHERE dt BETWEEN '{start_date}' AND '{end_date}';
DELETE FROM analytics.dim_time WHERE time_key BETWEEN '{start_date}' AND '{end_date}';

-- Analyze tables for performance
ANALYZE analytics.fact_rides;
ANALYZE analytics.dim_stations;
ANALYZE analytics.dim_time;
ANALYZE analytics.agg_daily_metrics;
"""
    
    elif cleanup_mode == "cascade_delete":
        sql = f"""
-- Cascade delete for date range: {start_date} to {end_date}
DELETE FROM analytics.fact_rides WHERE dt BETWEEN '{start_date}' AND '{end_date}';

-- Analyze tables for performance
ANALYZE analytics.fact_rides;
ANALYZE analytics.dim_stations;
ANALYZE analytics.dim_time;
"""
    
    else:
        raise ValueError(f"Unknown cleanup_mode: {cleanup_mode}")
    
    return sql

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python cleanup_postgres_sql.py <YYYYMM> <cleanup_mode>")
        print("cleanup_mode: date_range, truncate_all, cascade_delete")
        sys.exit(1)
    
    year_month = sys.argv[1]
    cleanup_mode = sys.argv[2]
    
    try:
        sql = generate_cleanup_sql(year_month, cleanup_mode)
        print(sql)
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)
