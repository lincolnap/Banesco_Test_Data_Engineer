-- Create Airflow Database
CREATE DATABASE airflow_db;

-- Create Airflow User
CREATE USER airflow_user WITH PASSWORD 'airflow_password123';

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow_user;

-- Grant schema privileges
\c airflow_db;
GRANT ALL ON SCHEMA public TO airflow_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow_user;
