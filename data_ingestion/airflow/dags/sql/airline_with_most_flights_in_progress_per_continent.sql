CREATE TABLE IF NOT EXISTS analytics_data.airline_with_most_active_flights_per_continent (
    id SERIAL PRIMARY KEY,
    airline_code VARCHAR(100),
    airline_name VARCHAR(100),
    continent VARCHAR(100),
    flight_count INT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Insert data into the table
INSERT INTO analytics_data.airline_with_most_active_flights_per_continent (airline_code, airline_name, continent, flight_count)
SELECT azt.airline_iata, al.name as airline_name, azt.origin_zone_name as continent, COUNT(DISTINCT f.id) as flight_count
FROM processed_data.airport_and_zone_transition azt
LEFT JOIN raw_data.airline al
ON al.code = azt.airline_iata
LEFT JOIN raw_data.flight f
ON f.id = azt.flight_id
WHERE f.time > EXTRACT(EPOCH FROM NOW() - INTERVAL '600 seconds')::BIGINT
    AND azt.origin_zone_name = azt.destination_zone_name
GROUP BY continent, azt.airline_iata, airline_name
ORDER BY flight_count DESC;
