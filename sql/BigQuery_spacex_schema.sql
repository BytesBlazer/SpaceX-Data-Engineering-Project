-- create roadter table TABLE
CREATE  TABLE SpaceX_dataset.Roadster (
    id INTEGER,
    name STRING(300),
    mass_kg NUMERIC(8, 2),
    apoapsis_au BIGNUMERIC(18, 15),
    periapsis_au BIGNUMERIC(18, 15),
    semi_major_axis BIGNUMERIC(20, 15),
    eccentricity_au BIGNUMERIC(20, 15),
    inclination BIGNUMERIC(20, 15),
    longitude BIGNUMERIC(20, 13),
    period_days BIGNUMERIC(20, 13),
    speed_kph BIGNUMERIC(20, 15),
    earth_distance_km BIGNUMERIC(20, 10),
    mars_distance_km BIGNUMERIC(20, 10)
);

-- create rocket  table 
CREATE TABLE SpaceX_dataset.Rockets (
    id STRING(25),
    name STRING(300),
    height NUMERIC(5, 2),
    diameter NUMERIC(5, 2),
    mass NUMERIC(10, 2),
    type STRING(300),
    active BOOLEAN,
    stages INTEGER,
    boosters INTEGER,
    cost_per_launch NUMERIC(10, 2),
    success_rate_pct INTEGER,
    country STRING(500),
    first_flight DATE
);

-- create launches table
CREATE TABLE SpaceX_dataset.Launches (
    id STRING(25),
    rocket STRING(25),
    success BOOLEAN,
    launchpad STRING(25),
    flight_number INTEGER,
    name STRING(500),
    date_utc TIMESTAMP,
    upcoming BOOLEAN,
    webcast STRING(300)
);

-- Create payloads table.
CREATE TABLE SpaceX_dataset.Payloads (
    id STRING(25),
    name STRING(500),
    type STRING(500),
    reused BOOLEAN,
    launch STRING(25),
    mass_kg NUMERIC(8, 2),
    orbit STRING(50),
    regime STRING(50),
    reference_type STRING(300),
    periapsis_km BIGNUMERIC(20, 10),
    apoapsis_km BIGNUMERIC(20, 10),
    inclination_deg NUMERIC(5, 2),
    period_min NUMERIC(8, 2),
    lifespan_years NUMERIC(16, 8)
);

CREATE TABLE SpaceX_dataset.Payloads_Manufacturer (
    id INTEGER,
    payload_id STRING(25),
    manufacturer STRING(300)
);

-- create history tableE SpaceX_dataset.
CREATE TABLE SpaceX_dataset.History (
    id STRING(25),
    title STRING(300),
    links STRING(1000),
    event_date_utc TIMESTAMP
);