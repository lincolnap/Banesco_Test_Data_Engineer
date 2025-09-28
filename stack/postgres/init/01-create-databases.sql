-- Create additional databases for the Banesco Data Engineering stack
CREATE DATABASE banesco_analytics;
CREATE DATABASE banesco_warehouse;

-- Create a user for application access
CREATE USER app_user WITH PASSWORD 'app_password123';
GRANT ALL PRIVILEGES ON DATABASE banesco_analytics TO app_user;
GRANT ALL PRIVILEGES ON DATABASE banesco_warehouse TO app_user;

-- Create a user for read-only access
CREATE USER readonly_user WITH PASSWORD 'readonly_password123';
GRANT CONNECT ON DATABASE banesco_analytics TO readonly_user;
GRANT CONNECT ON DATABASE banesco_warehouse TO readonly_user;
