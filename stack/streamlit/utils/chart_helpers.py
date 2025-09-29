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
