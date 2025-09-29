# 🚴 Divvy Bikes Analytics Dashboard

## 📋 Overview

This Streamlit dashboard provides comprehensive analytics for Divvy Bikes data, with a special focus on **Member Distance Analysis** as requested. The dashboard connects to PostgreSQL (port 5433) and displays key performance indicators, trends, and insights about bike usage patterns.

## 🎯 Key Features

### 📊 Member Distance Analysis (Main KPI)
- **Average Distance Comparison**: Members vs Casual users
- **Total Distance Metrics**: Aggregate distance analysis
- **Median Distance Analysis**: Statistical distribution insights
- **Duration vs Distance Correlation**: Ride efficiency analysis
- **Station Usage Diversity**: Unique stations used by each group

### 📈 Comprehensive Analytics
- **Temporal Analysis**: Daily, hourly, and weekly patterns
- **Geographic Analysis**: Station popularity and route analysis
- **User Behavior**: Time preferences and distance categories
- **Performance Metrics**: Database statistics and data quality

## 🏗️ Architecture

```
stack/streamlit/
├── app/
│   └── app.py                 # Main Streamlit application
├── utils/
│   ├── database_connector.py  # PostgreSQL connection handler
│   └── chart_helpers.py       # Chart creation utilities
├── config/
│   └── dashboard_config.py    # Configuration settings
├── requirements.txt           # Python dependencies
└── README.md                 # This file
```

## 🗄️ Database Schema

The dashboard connects to the `analytics` schema in PostgreSQL with the following key tables:

### Core Tables
- `analytics.fact_rides` - Main rides data
- `analytics.dim_stations` - Station information
- `analytics.dim_time` - Time dimension
- `analytics.agg_daily_metrics` - Daily aggregations
- `analytics.agg_user_behavior` - User behavior metrics

### Key Views
- `analytics.v_member_distance_kpi` - **Main KPI view for member distance analysis**
- `analytics.v_daily_trends` - Daily trend analysis
- `analytics.v_station_popularity` - Station performance metrics

## 🚀 Getting Started

### Prerequisites
- PostgreSQL running on port 5433
- Analytics schema and tables created
- Data loaded via Airflow pipeline

### Running the Dashboard
```bash
# Navigate to streamlit directory
cd stack/streamlit

# Install dependencies
pip install -r requirements.txt

# Run the dashboard
streamlit run app/app.py
```

### Docker Usage
```bash
# From project root
docker-compose up streamlit
```

## 📊 Dashboard Pages

### 1. 🏠 Dashboard
- System overview and monitoring
- Service status indicators
- Basic metrics and health checks

### 2. 🚴 Divvy Bikes Analytics
- **Overview Tab**: Member distance KPIs and station popularity
- **Temporal Analysis**: Daily trends, hourly distribution, weekly patterns
- **Geographic Analysis**: Top routes and distance categories
- **User Behavior**: Time of day preferences and patterns

### 3. 📊 Member Distance Analysis (Main Feature)
- **Key Performance Indicators**: Comparative metrics between Members and Casual users
- **Detailed Analysis Charts**: Comprehensive visualizations
- **Comparative Analysis Table**: Side-by-side metrics comparison
- **Key Insights**: Automated insights generation
- **Additional Analysis**: Interactive drill-down options

### 4. 🔍 Service Status
- Real-time service monitoring
- Connection status indicators
- Health check results

### 5. 🔗 Data Connectors
- Database connection examples
- Configuration templates
- Usage instructions

### 6. 📈 Monitoring
- System performance metrics
- Resource usage visualization
- Alert management

## 🎯 Member Distance KPI Features

### Primary Metrics
1. **Average Distance**: Members vs Casual users comparison
2. **Total Distance**: Aggregate distance by member type
3. **Median Distance**: Statistical distribution analysis
4. **Average Duration**: Ride time comparison
5. **Station Diversity**: Unique station usage patterns

### Visualizations
- **Multi-panel KPI Chart**: 4-panel comprehensive analysis
- **Comparative Bar Charts**: Side-by-side comparisons
- **Scatter Plot**: Duration vs Distance correlation
- **Trend Analysis**: Time-series patterns

### Insights Generation
- **Automated Insights**: AI-generated key findings
- **Comparative Analysis**: Statistical significance indicators
- **Behavioral Patterns**: Usage pattern identification

## 🔧 Configuration

### Database Connection
```python
DATABASE_CONFIG = {
    "host": "postgres",
    "port": "5433",
    "database": "banesco_test",
    "user": "postgres",
    "password": "postgres123",
    "schema": "analytics"
}
```

### Chart Configuration
- **Color Schemes**: Consistent branding across charts
- **Interactive Elements**: Hover, zoom, and filter capabilities
- **Responsive Design**: Adapts to different screen sizes

## 📈 Performance Features

### Caching
- **Data Caching**: 5-minute TTL for database queries
- **Resource Caching**: Cached database connections
- **Chart Caching**: Optimized chart rendering

### Optimization
- **Query Optimization**: Efficient SQL queries with proper indexing
- **Lazy Loading**: Data loaded on demand
- **Memory Management**: Efficient data handling

## 🔍 Monitoring & Alerts

### Health Checks
- **Database Connectivity**: Real-time connection status
- **Data Quality**: Missing data detection
- **Performance Metrics**: Response time monitoring

### Error Handling
- **Graceful Degradation**: Fallback options for failed queries
- **User Feedback**: Clear error messages and guidance
- **Recovery Options**: Retry mechanisms and alternative views

## 🚀 Future Enhancements

### Planned Features
1. **Real-time Updates**: Live data streaming
2. **Advanced Filtering**: Date range and custom filters
3. **Export Capabilities**: PDF and Excel export
4. **Mobile Optimization**: Responsive mobile design
5. **User Authentication**: Role-based access control

### Integration Opportunities
1. **Alert System**: Email/Slack notifications
2. **API Endpoints**: REST API for external access
3. **Machine Learning**: Predictive analytics
4. **Custom Reports**: Automated report generation

## 📞 Support

For issues or questions:
1. Check the database connection and data availability
2. Verify the analytics schema is properly created
3. Ensure the Airflow pipeline has successfully loaded data
4. Review the logs for any error messages

## 🏷️ Version

- **Current Version**: 1.0.0
- **Last Updated**: 2024-01-15
- **Compatibility**: Python 3.11+, Streamlit 1.29.0+
