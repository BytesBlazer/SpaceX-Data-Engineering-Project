-- Delete table schema and its data if exists
DROP TABLE IF EXISTS Roadster;
DROP TABLE IF EXISTS Payloads_Manufacturer;
DROP TABLE IF EXISTS Payloads;
DROP TABLE IF EXISTS Launches;
DROP TABLE IF EXISTS Rockets;
DROP TABLE IF EXISTS History;

-- create roadter fact table
CREATE TABLE Roadster (
    id INTEGER PRIMARY KEY,
    name VARCHAR(300),
    mass_kg NUMERIC(8, 2),
    apoapsis_au NUMERIC(18, 15),
    periapsis_au NUMERIC(18, 15),
    semi_major_axis NUMERIC(20, 15),
    eccentricity_au NUMERIC(20, 15),
    inclination NUMERIC(20, 15),
    longitude NUMERIC(20, 13),
    period_days NUMERIC(20, 13),
    speed_kph NUMERIC(20, 15),
    earth_distance_km NUMERIC(20, 10),
    mars_distance_km NUMERIC(20, 10)
);

-- create rocket table
CREATE TABLE Rockets (
    id CHAR(25) PRIMARY KEY,
    name VARCHAR(300),
    height NUMERIC(5, 2),
    diameter NUMERIC(5, 2),
    mass NUMERIC(10, 2),
    type VARCHAR(300),
    active BOOLEAN,
    stages INTEGER,
    boosters INTEGER,
    cost_per_launch NUMERIC(10, 2),
    success_rate_pct INTEGER,
    country VARCHAR(500),
    first_flight DATE
);

-- create launches table
CREATE TABLE Launches (
    id CHAR(25) PRIMARY KEY,
    rocket CHAR(25),
    success BOOLEAN,
    launchpad CHAR(25),
    flight_number INTEGER,
    name VARCHAR(500),
    date_utc TIMESTAMP WITH TIME ZONE,
    upcoming BOOLEAN,
    webcast VARCHAR(300)
);

-- Create payloads table
CREATE TABLE Payloads (
    id CHAR(25) PRIMARY KEY,
    name VARCHAR(500),
    type VARCHAR(500),
    reused BOOLEAN,
    launch CHAR(25),
    mass_kg NUMERIC(8, 2),
    orbit VARCHAR(50),
    regime VARCHAR(50),
    reference_type VARCHAR(300),
    periapsis_km NUMERIC(20, 10),
    apoapsis_km NUMERIC(20, 10),
    inclination_deg NUMERIC(5, 2),
    period_min NUMERIC(8, 2),
    lifespan_years NUMERIC(16, 8)
);

CREATE TABLE Payloads_Manufacturer (
    id INTEGER PRIMARY KEY,
    payload_id CHAR(25),
    manufacturer VARCHAR(300)
);

-- create history table
CREATE TABLE History (
    id CHAR(25) PRIMARY KEY,
    title VARCHAR(300),
    links VARCHAR(1000),
    event_date_utc TIMESTAMP WITH TIME ZONE
);

-- change directory into spacex home directory
\cd /home/osairis/Data_Engineering/SpaceX/