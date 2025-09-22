-- Create schemas
CREATE SCHEMA IF NOT EXISTS source_schema;
CREATE SCHEMA IF NOT EXISTS staging_schema;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Create source table with sample data
CREATE TABLE IF NOT EXISTS source_schema.users (
    user_id SERIAL PRIMARY KEY,
    username VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    country VARCHAR(100),
    age INTEGER
);

-- Insert sample data
INSERT INTO source_schema.users (username, email, country, age) 
SELECT 
    'user_' || generate_series,
    'user_' || generate_series || '@example.com',
    (ARRAY['USA', 'UK', 'Canada', 'Australia', 'Germany'])[1 + random() * 4],
    20 + (random() * 50)::int
FROM generate_series(1, 100);

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA source_schema TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging_schema TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA analytics TO postgres;