#!/bin/bash

# Airflow Entrypoint Script
# This script runs the complete setup and then starts the requested Airflow service

set -e

echo "🚀 Starting Airflow Container Setup..."
echo "====================================="

# Function to run setup in background
run_setup_background() {
    echo "🔧 Running Airflow setup in background..."
    python3 /opt/airflow/scripts/setup_complete.py &
    SETUP_PID=$!
    echo "📋 Setup process started with PID: $SETUP_PID"
}

# Function to wait for setup completion
wait_for_setup() {
    if [ ! -z "$SETUP_PID" ]; then
        echo "⏳ Waiting for setup to complete..."
        wait $SETUP_PID
        SETUP_EXIT_CODE=$?
        if [ $SETUP_EXIT_CODE -eq 0 ]; then
            echo "✅ Setup completed successfully!"
        else
            echo "⚠️ Setup completed with warnings (exit code: $SETUP_EXIT_CODE)"
        fi
    fi
}

# Check if this is the webserver or scheduler
if [ "$1" = "webserver" ] || [ "$1" = "scheduler" ]; then
    echo "🎯 Starting $1 with automatic setup..."
    
    # Run setup in background for webserver/scheduler
    run_setup_background
    
    # Start the requested service
    echo "🚀 Starting Airflow $1..."
    exec airflow "$@"
    
elif [ "$1" = "standalone" ]; then
    echo "🎯 Starting Airflow standalone with setup..."
    
    # Run setup first, then start standalone
    echo "🔧 Running setup before starting standalone..."
    python3 /opt/airflow/scripts/setup_complete.py
    
    echo "🚀 Starting Airflow standalone..."
    exec airflow "$@"
    
else
    # For other commands, just run them directly
    echo "🎯 Running command: $@"
    exec "$@"
fi
