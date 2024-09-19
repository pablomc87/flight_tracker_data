CREATE TABLE IF NOT EXISTS processed_data.airport_and_zone_transition (
    id SERIAL PRIMARY KEY,
    flight_id VARCHAR(100),
    airline_iata VARCHAR(100),
    airline_icao VARCHAR(100),
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

INSERT INTO processed_data.airport_and_zone_transition (
    flight_id,
    airline_iata,
    airline_icao,
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
    f.id,
    f.airline_iata,
    f.airline_icao,
    oa.name as origin_airport_name,
    oa.latitude as origin_airport_latitude,
    oa.longitude as origin_airport_longitude,
    oa.continent as origin_zone_name,
    da.name as destination_airport_name,
    da.latitude as destination_airport_latitude,
    da.longitude as destination_airport_longitude,
    da.continent as destination_zone_name,
    -- Using POSTGIS function to calculate the distance between two points
    ST_Distance(
        ST_SetSRID(ST_MakePoint(oa.longitude, oa.latitude), 4326)::geography,
        ST_SetSRID(ST_MakePoint(da.longitude, da.latitude), 4326)::geography
    ) / 1000 AS distance_km
FROM raw_data.flight f
    JOIN raw_data.airport oa
        ON f.origin_airport_iata = oa.iata
    JOIN raw_data.airport da
        ON f.destination_airport_iata = da.iata
WHERE f.time > EXTRACT(EPOCH FROM NOW() - INTERVAL '300 seconds')::BIGINT
;

