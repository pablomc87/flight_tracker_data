CREATE TABLE IF NOT EXISTS analytics_data.current_longest_flight (
    id SERIAL PRIMARY KEY,
    flight_id VARCHAR(100),
    airline_iata VARCHAR(100),
    origin_airport_name VARCHAR(100),
    origin_airport_latitude REAL,
    origin_airport_longitude REAL,
    origin_zone_name VARCHAR(100),
    destination_airport_name VARCHAR(100),
    destination_airport_latitude REAL,
    destination_airport_longitude REAL,
    destination_zone_name VARCHAR(100),
    distance_km REAL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO analytics_data.current_longest_flight (
    flight_id,
    airline_iata,
    origin_airport_name,
    origin_airport_latitude,
    origin_airport_longitude,
    origin_zone_name,
    destination_airport_name,
    destination_airport_latitude,
    destination_airport_longitude,
    destination_zone_name,
    distance_km
    )
SELECT
    flight_id,
    airline_iata,
    origin_airport_name,
    origin_airport_latitude,
    origin_airport_longitude,
    origin_zone_name,
    destination_airport_name,
    destination_airport_latitude,
    destination_airport_longitude,
    destination_zone_name,
    distance_km
FROM processed_data.airport_and_zone_transition
ORDER BY created_at DESC, distance_km DESC
LIMIT 1;