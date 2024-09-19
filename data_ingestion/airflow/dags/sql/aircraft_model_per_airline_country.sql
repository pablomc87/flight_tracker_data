CREATE TABLE IF NOT EXISTS processed_data.aircraft_model_per_airline_country (
    id SERIAL PRIMARY KEY,
    airline_name VARCHAR(255) NOT NULL,
    country VARCHAR(255) NOT NULL,
    model_name VARCHAR(255) NOT NULL,
    aircraft_count INT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO processed_data.aircraft_model_per_airline_country (airline_name, country, model_name, aircraft_count)
SELECT
    a.name,
    a.country,
    f.aircraft_model,
    COUNT(*) AS aircraft_count
FROM
    raw_data.airline a
JOIN
    raw_data.flight f ON f.airline_icao = a.icao
GROUP BY
    a.name, a.country, f.aircraft_model
ORDER BY
    a.country, a.name, f.aircraft_model;