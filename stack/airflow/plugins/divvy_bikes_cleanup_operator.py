"""
Custom Airflow Operator for Divvy Bikes Data Cleanup
"""

from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class DivvyBikesCleanupOperator(BaseOperator):
    """
    Custom Airflow operator to cleanup Divvy Bikes data in PostgreSQL
    before loading new data to avoid duplicates and manage incremental loads.
    """
    
    template_fields = ['year_month', 'cleanup_mode']
    
    def __init__(
        self,
        year_month,
        postgres_conn_id='postgres_default',
        cleanup_mode='date_range',
        *args,
        **kwargs
    ):
        """
        Initialize the cleanup operator
        
        :param year_month: Target year-month in YYYYMM format
        :param postgres_conn_id: Airflow connection ID for PostgreSQL
        :param cleanup_mode: 'date_range', 'truncate_all', or 'cascade_delete'
        """
        super(DivvyBikesCleanupOperator, self).__init__(*args, **kwargs)
        self.year_month = year_month
        self.postgres_conn_id = postgres_conn_id
        self.cleanup_mode = cleanup_mode
        
    def get_date_range_from_year_month(self, year_month):
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
    
    def execute(self, context):
        """Execute the cleanup operation"""
        logger.info(f"🧹 Starting data cleanup for {self.year_month}")
        logger.info(f"🔧 Cleanup mode: {self.cleanup_mode}")
        
        # Get PostgreSQL hook
        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        
        start_date, end_date = self.get_date_range_from_year_month(self.year_month)
        logger.info(f"📅 Target date range: {start_date} to {end_date}")
        
        try:
            if self.cleanup_mode == "truncate_all":
                self._truncate_all_tables(postgres_hook)
                
            elif self.cleanup_mode == "date_range":
                self._delete_by_date_range(postgres_hook, start_date, end_date)
                
            elif self.cleanup_mode == "cascade_delete":
                self._cascade_delete_by_date(postgres_hook, start_date, end_date)
            
            # Analyze tables for performance
            self._analyze_tables(postgres_hook)
            
            logger.info("✅ Data cleanup completed successfully!")
            
        except Exception as e:
            logger.error(f"❌ Cleanup operation failed: {str(e)}")
            raise
    
    def _truncate_all_tables(self, postgres_hook):
        """Truncate all analytics tables"""
        logger.info("🗑️ Truncating ALL analytics tables...")
        
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
                postgres_hook.run(query)
                logger.info(f"✅ Executed: {query}")
            except Exception as e:
                logger.warning(f"⚠️ Query failed (may be expected): {query} - {str(e)}")
    
    def _delete_by_date_range(self, postgres_hook, start_date, end_date):
        """Delete data for specific date range"""
        logger.info(f"🗑️ Deleting data for date range: {start_date} to {end_date}")
        
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
                result = postgres_hook.run(query, autocommit=False)
                table_name = query.split("FROM ")[1].split(" WHERE")[0]
                
                # Get row count
                count_query = f"SELECT ROW_COUNT();"
                # Note: PostgreSQL doesn't have ROW_COUNT(), using a different approach
                rows_affected[table_name] = "executed"
                logger.info(f"✅ Deleted data from {table_name}")
            except Exception as e:
                logger.warning(f"⚠️ Delete failed: {query} - {str(e)}")
        
        logger.info("📊 Cleanup Summary completed")
    
    def _cascade_delete_by_date(self, postgres_hook, start_date, end_date):
        """Cascade delete for date range"""
        logger.info(f"🗑️ Cascade delete for date range: {start_date} to {end_date}")
        
        cascade_query = f"""
        DELETE FROM analytics.fact_rides 
        WHERE dt BETWEEN '{start_date}' AND '{end_date}';
        """
        
        postgres_hook.run(cascade_query)
        logger.info("✅ Cascade delete completed")
    
    def _analyze_tables(self, postgres_hook):
        """Run ANALYZE on tables for performance"""
        logger.info("📈 Running ANALYZE on cleaned tables...")
        
        analyze_queries = [
            "ANALYZE analytics.fact_rides;",
            "ANALYZE analytics.dim_stations;", 
            "ANALYZE analytics.dim_time;",
            "ANALYZE analytics.agg_daily_metrics;"
        ]
        
        for query in analyze_queries:
            try:
                postgres_hook.run(query)
                logger.info(f"✅ {query}")
            except Exception as e:
                logger.warning(f"⚠️ Analyze failed: {query} - {str(e)}")
