"""
Chart Helpers for Streamlit Dashboard
Utility functions for creating charts and visualizations
"""

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np

def create_member_distance_kpi_chart(df):
    """Create KPI chart for member distance analysis"""
    if df.empty:
        return go.Figure()
    
    # Create subplots
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Average Distance by Member Type', 'Total Distance by Member Type', 
                       'Median Distance Comparison', 'Ride Duration vs Distance'),
        specs=[[{"type": "bar"}, {"type": "bar"}],
               [{"type": "bar"}, {"type": "scatter"}]]
    )
    
    # Average Distance
    fig.add_trace(
        go.Bar(x=df['member_casual'], y=df['avg_distance_km'], 
               name='Avg Distance', marker_color=['#1f77b4', '#ff7f0e']),
        row=1, col=1
    )
    
    # Total Distance
    fig.add_trace(
        go.Bar(x=df['member_casual'], y=df['total_distance_km'], 
               name='Total Distance', marker_color=['#2ca02c', '#d62728']),
        row=1, col=2
    )
    
    # Median Distance
    fig.add_trace(
        go.Bar(x=df['member_casual'], y=df['median_distance_km'], 
               name='Median Distance', marker_color=['#9467bd', '#8c564b']),
        row=2, col=1
    )
    
    # Duration vs Distance scatter
    fig.add_trace(
        go.Scatter(x=df['avg_duration_minutes'], y=df['avg_distance_km'],
                  mode='markers+text', text=df['member_casual'],
                  textposition="top center", name='Duration vs Distance',
                  marker=dict(size=15, color=['#1f77b4', '#ff7f0e'])),
        row=2, col=2
    )
    
    fig.update_layout(
        title="Member Distance Analysis - Key Performance Indicators",
        showlegend=False,
        height=600
    )
    
    return fig

def create_daily_trends_chart(df):
    """Create daily trends chart"""
    if df.empty:
        return go.Figure()
    
    fig = go.Figure()
    
    for member_type in df['member_casual'].unique():
        member_data = df[df['member_casual'] == member_type]
        fig.add_trace(go.Scatter(
            x=member_data['dt'],
            y=member_data['total_daily_distance'],
            mode='lines+markers',
            name=f'{member_type} - Total Distance',
            line=dict(width=3)
        ))
    
    fig.update_layout(
        title="Daily Distance Trends by Member Type",
        xaxis_title="Date",
        yaxis_title="Total Distance (km)",
        hovermode='x unified',
        height=400
    )
    
    return fig

def create_hourly_distribution_chart(df):
    """Create hourly distribution chart"""
    if df.empty:
        return go.Figure()
    
    fig = go.Figure()
    
    for member_type in df['member_casual'].unique():
        member_data = df[df['member_casual'] == member_type]
        fig.add_trace(go.Scatter(
            x=member_data['hour_of_day'],
            y=member_data['ride_count'],
            mode='lines+markers',
            name=member_type,
            line=dict(width=3)
        ))
    
    fig.update_layout(
        title="Hourly Ride Distribution by Member Type",
        xaxis_title="Hour of Day",
        yaxis_title="Number of Rides",
        xaxis=dict(dtick=1),
        height=400
    )
    
    return fig

def create_distance_category_chart(df):
    """Create distance category analysis chart"""
    if df.empty:
        return go.Figure()
    
    # Pivot data for grouped bar chart
    pivot_df = df.pivot(index='distance_category', columns='member_casual', values='ride_count').fillna(0)
    
    fig = go.Figure()
    
    for member_type in pivot_df.columns:
        fig.add_trace(go.Bar(
            x=pivot_df.index,
            y=pivot_df[member_type],
            name=member_type,
            text=pivot_df[member_type],
            textposition='auto'
        ))
    
    fig.update_layout(
        title="Ride Distribution by Distance Category",
        xaxis_title="Distance Category",
        yaxis_title="Number of Rides",
        barmode='group',
        height=400
    )
    
    return fig

def create_time_of_day_chart(df):
    """Create time of day analysis chart"""
    if df.empty:
        return go.Figure()
    
    fig = go.Figure()
    
    for member_type in df['member_casual'].unique():
        member_data = df[df['member_casual'] == member_type]
        fig.add_trace(go.Bar(
            x=member_data['time_of_day'],
            y=member_data['avg_distance'],
            name=member_type,
            text=member_data['avg_distance'].round(2),
            textposition='auto'
        ))
    
    fig.update_layout(
        title="Average Distance by Time of Day",
        xaxis_title="Time of Day",
        yaxis_title="Average Distance (km)",
        barmode='group',
        height=400
    )
    
    return fig

def create_weekly_patterns_chart(df):
    """Create weekly patterns chart"""
    if df.empty:
        return go.Figure()
    
    # Map day numbers to names
    day_names = {1: 'Sunday', 2: 'Monday', 3: 'Tuesday', 4: 'Wednesday', 
                 5: 'Thursday', 6: 'Friday', 7: 'Saturday'}
    
    df['day_name'] = df['day_of_week'].map(day_names)
    
    fig = go.Figure()
    
    for member_type in df['member_casual'].unique():
        member_data = df[df['member_casual'] == member_type]
        fig.add_trace(go.Scatter(
            x=member_data['day_name'],
            y=member_data['ride_count'],
            mode='lines+markers',
            name=member_type,
            line=dict(width=3)
        ))
    
    fig.update_layout(
        title="Weekly Ride Patterns by Member Type",
        xaxis_title="Day of Week",
        yaxis_title="Number of Rides",
        height=400
    )
    
    return fig

def create_station_popularity_chart(df):
    """Create station popularity chart"""
    if df.empty:
        return go.Figure()
    
    # Take top 10 stations
    top_stations = df.head(10)
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=top_stations['station_name'],
        y=top_stations['total_rides'],
        name='Total Rides',
        marker_color='lightblue',
        text=top_stations['total_rides'],
        textposition='auto'
    ))
    
    fig.update_layout(
        title="Top 10 Most Popular Stations",
        xaxis_title="Station Name",
        yaxis_title="Number of Rides",
        xaxis_tickangle=-45,
        height=400
    )
    
    return fig

def create_top_routes_chart(df):
    """Create top routes chart"""
    if df.empty:
        return go.Figure()
    
    # Create route labels
    df['route'] = df['start_station_name'] + ' → ' + df['end_station_name']
    
    # Take top 10 routes
    top_routes = df.head(10)
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=top_routes['route'],
        y=top_routes['ride_count'],
        name='Ride Count',
        marker_color='lightgreen',
        text=top_routes['ride_count'],
        textposition='auto'
    ))
    
    fig.update_layout(
        title="Top 10 Most Popular Routes",
        xaxis_title="Route",
        yaxis_title="Number of Rides",
        xaxis_tickangle=-45,
        height=500
    )
    
    return fig

def create_database_stats_cards(df):
    """Create database statistics cards"""
    if df.empty:
        return {}
    
    stats = {}
    for _, row in df.iterrows():
        stats[row['metric']] = row['value']
    
    return stats

def create_quarter_member_chart(df, quarter_text):
    """Create quarter analysis chart by member type"""
    if df.empty:
        return go.Figure()
    
    fig = go.Figure()
    
    # Add bar chart for total rides
    fig.add_trace(go.Bar(
        x=df['member_casual'],
        y=df['total_rides'],
        name='Total Rides',
        marker_color=['#1f77b4', '#ff7f0e'],
        text=df['total_rides'],
        textposition='auto',
    ))
    
    fig.update_layout(
        title=f'{quarter_text} Rides by Member Type',
        xaxis_title='Member Type',
        yaxis_title='Total Rides',
        showlegend=False,
        height=400
    )
    
    return fig

def create_quarter_monthly_chart(df, quarter_text):
    """Create quarter monthly trend chart"""
    if df.empty:
        return go.Figure()
    
    fig = go.Figure()
    
    # Add line chart for monthly trend
    fig.add_trace(go.Scatter(
        x=df['month_name'],
        y=df['total_rides'],
        mode='lines+markers+text',
        name='Total Rides',
        line=dict(color='#1f77b4', width=3),
        marker=dict(size=10),
        text=df['total_rides'],
        textposition='top center'
    ))
    
    # Add bar chart for total distance (if available)
    if 'total_distance' in df.columns:
        fig.add_trace(go.Bar(
            x=df['month_name'],
            y=df['total_distance'],
            name='Total Distance (km)',
            yaxis='y2',
            opacity=0.7,
            marker_color='#ff7f0e'
        ))
    
    fig.update_layout(
        title=f'{quarter_text} Monthly Trend: Rides & Distance',
        xaxis_title='Month',
        yaxis_title='Total Rides',
        yaxis2=dict(
            title='Total Distance (km)',
            overlaying='y',
            side='right'
        ) if 'total_distance' in df.columns else None,
        height=400,
        hovermode='x unified'
    )
    
    return fig

def create_comparison_chart(df, comparison_type):
    """Create comparison chart based on selected type"""
    if df.empty:
        return go.Figure()
    
    fig = go.Figure()
    
    # Determine chart type based on comparison
    if "Días" in comparison_type:
        title = "Daily Rides Comparison (Last 30 Days)"
        x_col = 'period_label'
    elif "Meses" in comparison_type:
        title = "Monthly Rides Comparison"
        x_col = 'period_label'
    elif "Cuartiles" in comparison_type:
        title = "Quarterly Rides Comparison"
        x_col = 'period_label'
    else:  # Años
        title = "Yearly Rides Comparison"
        x_col = 'period_label'
    
    # Add line chart
    fig.add_trace(go.Scatter(
        x=df[x_col],
        y=df['total_rides'],
        mode='lines+markers',
        name='Total Rides',
        line=dict(color='#1f77b4', width=2),
        marker=dict(size=6),
        hovertemplate='<b>%{x}</b><br>Rides: %{y:,}<extra></extra>'
    ))
    
    # Add trend line if enough data points
    if len(df) > 3:
        z = np.polyfit(range(len(df)), df['total_rides'], 1)
        trend_line = np.poly1d(z)(range(len(df)))
        
        fig.add_trace(go.Scatter(
            x=df[x_col],
            y=trend_line,
            mode='lines',
            name='Trend',
            line=dict(color='red', width=2, dash='dash'),
            hovertemplate='Trend: %{y:,.0f}<extra></extra>'
        ))
    
    fig.update_layout(
        title=title,
        xaxis_title='Period',
        yaxis_title='Total Rides',
        height=400,
        hovermode='x unified'
    )
    
    # Rotate x-axis labels if too many data points
    if len(df) > 10:
        fig.update_xaxes(tickangle=45)
    
    return fig

def analyze_growth_trend(df):
    """Analyze growth trend from comparison data"""
    if df.empty or len(df) < 2:
        return {
            'direction': 'Insufficient Data',
            'avg_growth': 0,
            'periods': len(df)
        }
    
    # Calculate period-over-period growth rates
    growth_rates = []
    for i in range(1, len(df)):
        current = df.iloc[i]['total_rides']
        previous = df.iloc[i-1]['total_rides']
        if previous > 0:
            growth_rate = ((current - previous) / previous) * 100
            growth_rates.append(growth_rate)
    
    if not growth_rates:
        return {
            'direction': 'No Growth Data',
            'avg_growth': 0,
            'periods': len(df)
        }
    
    avg_growth = np.mean(growth_rates)
    
    # Determine trend direction
    if avg_growth > 5:
        direction = "📈 Growing"
    elif avg_growth > 0:
        direction = "📊 Stable Growth"
    elif avg_growth > -5:
        direction = "📉 Slight Decline"
    else:
        direction = "📉 Declining"
    
    return {
        'direction': direction,
        'avg_growth': avg_growth,
        'periods': len(df)
    }
