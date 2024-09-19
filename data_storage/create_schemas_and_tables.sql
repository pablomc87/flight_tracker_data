SET timezone = 'Europe/Paris';

CREATE SCHEMA IF NOT EXISTS raw_data;
CREATE SCHEMA IF NOT EXISTS processed_data;
CREATE SCHEMA IF NOT EXISTS analytics_data;

CREATE EXTENSION IF NOT EXISTS postgis;

-- Create table airlines in raw_data schema
CREATE TABLE IF NOT EXISTS raw_data.airline (
    icao TEXT NOT NULL PRIMARY KEY,
    code TEXT,
    name TEXT,
    country TEXT
);

CREATE TABLE IF NOT EXISTS raw_data.flight (
    id                          TEXT NOT NULL PRIMARY KEY,
    aircraft_code               TEXT,
    airline_iata                TEXT,
    airline_icao                TEXT,
    altitude                    INT,
    callsign                    TEXT,
    destination_airport_iata    TEXT,
    ground_speed                INT,
    heading                     INT,
    icao_24bit                  TEXT,
    latitude                    REAL,
    longitude                   REAL,
    number                      TEXT,
    on_ground                   INT,
    origin_airport_iata         TEXT,
    registration                TEXT,
    squawk                      TEXT,
    time                        INT,
    vertical_speed              INT,
    aircraft_model              TEXT
);

CREATE TABLE IF NOT EXISTS raw_data.airport (
    icao		TEXT NOT NULL PRIMARY KEY,
    altitude	INT,
    country		TEXT,
    iata		TEXT,
    latitude	REAL,
    longitude	REAL,
    name		TEXT,
    continent   TEXT
);