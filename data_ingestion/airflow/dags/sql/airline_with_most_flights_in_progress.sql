CREATE TABLE IF NOT EXISTS processed_data.most_flights_in_progress (
    id SERIAL PRIMARY KEY,
    airline_code VARCHAR(100),
    airline_name VARCHAR(100),
    flight_count INT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Insert data into the table
INSERT INTO processed_data.most_flights_in_progress (airline_code, airline_name, flight_count)
SELECT fl.airline_iata, al.name, COUNT(DISTINCT fl.id) as flight_count
FROM raw_data.flight fl
LEFT JOIN raw_data.airline al
ON al.code = fl.airline_iata
WHERE time > EXTRACT(EPOCH FROM NOW() - INTERVAL '600 seconds')::BIGINT and on_ground = 0
AND airline_iata != ''
GROUP BY fl.airline_iata, al.name
ORDER BY flight_count DESC;
