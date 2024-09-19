CREATE TABLE IF NOT EXISTS analytics_data.average_flight_distance_per_continent (
    id SERIAL PRIMARY KEY,
    continent VARCHAR(100),
    average_flight_distance INT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TEMP TABLE IF NOT EXISTS airport_and_zone_transition_unique AS
SELECT DISTINCT ON (flight_id) azt.*
FROM processed_data.airport_and_zone_transition azt
LEFT JOIN raw_data.flight f
ON f.id = azt.flight_id
WHERE f.time > EXTRACT(EPOCH FROM NOW() - INTERVAL '600 seconds')::BIGINT
;

-- Insert data into the table
INSERT INTO analytics_data.average_flight_distance_per_continent (continent, average_flight_distance)
SELECT aztu.origin_zone_name as continent, AVG(distance_km) as average_flight_distance
FROM airport_and_zone_transition_unique aztu
LEFT JOIN raw_data.flight f
ON f.id = aztu.flight_id
GROUP BY continent;