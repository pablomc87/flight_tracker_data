CREATE TABLE IF NOT EXISTS analytics_data.most_active_flights_per_builder (
    id SERIAL PRIMARY KEY,
    builder TEXT,
    flight_count INT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Create a temporary table to count flights per builder
CREATE TEMP TABLE flight_counts_per_builder AS
SELECT
    split_part(f.aircraft_model, ' ', 1) AS builder,
    COUNT(f.id) AS flight_count
FROM
    raw_data.flight f
GROUP BY
    builder
ORDER BY
    flight_count DESC;

-- Insert the results into a table in the analytics_data schema
INSERT INTO analytics_data.most_active_flights_per_builder (
    builder,
    flight_count
)
SELECT
    builder,
    flight_count
FROM
    flight_counts_per_builder;