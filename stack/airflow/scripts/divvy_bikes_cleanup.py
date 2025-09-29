#!/usr/bin/env python3
"""
Divvy Bikes Data Cleanup - Airflow Task
Truncate or delete PostgreSQL data based on the date range being inserted
"""

import os
import sys
import argparse
import psycopg2
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
        logger.error(f"Error parsing year_month {year_month}: {str(e)}")
        raise

def cleanup_postgres_data(year_month, postgres_config, cleanup_mode="date_range"):
    """
    Clean up PostgreSQL data based on the specified date range
    
    cleanup_mode options:
    - "date_range": Delete only data for the specific month
    - "truncate_all": Truncate all tables (full reload)
    - "cascade_delete": Delete with foreign key cascade
    """
    
    start_date, end_date = get_date_range_from_year_month(year_month)
    logger.info(f"Cleanup mode: {cleanup_mode}")
    logger.info(f"Date range: {start_date} to {end_date}")
    
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=postgres_config["host"],
            port=postgres_config["port"],
            database=postgres_config["database"],
            user=postgres_config["user"],
            password=postgres_config["password"]
        )
        
        cursor = conn.cursor()
        
        if cleanup_mode == "truncate_all":
            logger.info("🗑️ Truncating ALL analytics tables...")
            
            # Truncate in correct order due to foreign keys
            truncate_queries = [
                "TRUNCATE TABLE analytics.agg_user_behavior CASCADE;",
                "TRUNCATE TABLE analytics.agg_station_metrics CASCADE;", 
                "TRUNCATE TABLE analytics.agg_geographical_analysis CASCADE;",
                "TRUNCATE TABLE analytics.agg_daily_metrics CASCADE;",
                "TRUNCATE TABLE analytics.fact_rides CASCADE;",
                "TRUNCATE TABLE analytics.dim_stations CASCADE;",
                "TRUNCATE TABLE analytics.dim_time CASCADE;"
            ]
            
            for query in truncate_queries:
                try:
                    cursor.execute(query)
                    logger.info(f"✅ Executed: {query}")
                except Exception as e:
                    logger.warning(f"⚠️ Query failed (may be expected): {query} - {str(e)}")
            
        elif cleanup_mode == "date_range":
            logger.info(f"🗑️ Deleting data for date range: {start_date} to {end_date}")
            
            # Delete in correct order due to foreign keys
            delete_queries = [
                f"DELETE FROM analytics.agg_user_behavior WHERE date_key BETWEEN '{start_date}' AND '{end_date}';",
                f"DELETE FROM analytics.agg_station_metrics WHERE date_key BETWEEN '{start_date}' AND '{end_date}';",
                f"DELETE FROM analytics.agg_geographical_analysis WHERE date_key BETWEEN '{start_date}' AND '{end_date}';", 
                f"DELETE FROM analytics.agg_daily_metrics WHERE date_key BETWEEN '{start_date}' AND '{end_date}';",
                f"DELETE FROM analytics.fact_rides WHERE dt BETWEEN '{start_date}' AND '{end_date}';",
                f"DELETE FROM analytics.dim_time WHERE time_key BETWEEN '{start_date}' AND '{end_date}';"
            ]
            
            rows_affected = {}
            for query in delete_queries:
                try:
                    cursor.execute(query)
                    affected = cursor.rowcount
                    table_name = query.split("FROM ")[1].split(" WHERE")[0]
                    rows_affected[table_name] = affected
                    logger.info(f"✅ Deleted {affected} rows from {table_name}")
                except Exception as e:
                    logger.warning(f"⚠️ Delete failed: {query} - {str(e)}")
                    
            # Summary
            logger.info("📊 Cleanup Summary:")
            for table, count in rows_affected.items():
                logger.info(f"   {table}: {count:,} rows deleted")
        
        elif cleanup_mode == "cascade_delete":
            logger.info(f"🗑️ Cascade delete for date range: {start_date} to {end_date}")
            
            # Start with fact table - let foreign keys cascade
            cascade_query = f"""
            DELETE FROM analytics.fact_rides 
            WHERE dt BETWEEN '{start_date}' AND '{end_date}';
            """
            
            cursor.execute(cascade_query)
            affected = cursor.rowcount
            logger.info(f"✅ Cascade deleted {affected} rows from fact_rides")
        
        # Commit changes
        conn.commit()
        logger.info("✅ All cleanup operations committed successfully")
        
        # Analyze tables for performance
        analyze_queries = [
            "ANALYZE analytics.fact_rides;",
            "ANALYZE analytics.dim_stations;", 
            "ANALYZE analytics.dim_time;",
            "ANALYZE analytics.agg_daily_metrics;"
        ]
        
        logger.info("📈 Running ANALYZE on cleaned tables...")
        for query in analyze_queries:
            try:
                cursor.execute(query)
                logger.info(f"✅ {query}")
            except Exception as e:
                logger.warning(f"⚠️ Analyze failed: {query} - {str(e)}")
        
        conn.commit()
        
    except Exception as e:
        logger.error(f"❌ Error during cleanup: {str(e)}")
        if 'conn' in locals():
            conn.rollback()
        raise
        
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
        logger.info("🔌 Database connection closed")

def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description='Cleanup Divvy Bikes data in PostgreSQL')
    parser.add_argument('--year-month', required=True, help='Year-Month in format YYYYMM')
    parser.add_argument('--cleanup-mode', 
                       choices=['date_range', 'truncate_all', 'cascade_delete'],
                       default='date_range',
                       help='Cleanup mode: date_range (default), truncate_all, or cascade_delete')
    parser.add_argument('--postgres-host', default='postgres', help='PostgreSQL host')
    parser.add_argument('--postgres-port', default='5432', help='PostgreSQL port')
    parser.add_argument('--postgres-db', default='banesco_test', help='PostgreSQL database')
    parser.add_argument('--postgres-user', default='postgres', help='PostgreSQL user')
    parser.add_argument('--postgres-password', default='postgres123', help='PostgreSQL password')
    
    args = parser.parse_args()
    
    postgres_config = {
        "host": args.postgres_host,
        "port": args.postgres_port,
        "database": args.postgres_db,
        "user": args.postgres_user,
        "password": args.postgres_password
    }
    
    try:
        logger.info("🧹 Starting Divvy Bikes data cleanup...")
        logger.info(f"📅 Target period: {args.year_month}")
        logger.info(f"🔧 Cleanup mode: {args.cleanup_mode}")
        
        cleanup_postgres_data(args.year_month, postgres_config, args.cleanup_mode)
        
        logger.info("✅ Data cleanup completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Cleanup failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
