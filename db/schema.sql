-- nextbike prague database schema

CREATE EXTENSION IF NOT EXISTS postgis;

-- one row per station from the nextbike api
-- is_spot: marked pickup area on the map (most stations are this)
-- is_bike: virtual spot that moves with a single free-floating bike
CREATE TABLE stations (
    uid           INTEGER PRIMARY KEY,
    name          TEXT NOT NULL,
    lat           DOUBLE PRECISION NOT NULL,
    lng           DOUBLE PRECISION NOT NULL,
    bike_racks    INTEGER,
    is_spot       BOOLEAN NOT NULL DEFAULT TRUE,
    is_bike       BOOLEAN NOT NULL DEFAULT FALSE,
    first_seen_at TIMESTAMPTZ NOT NULL
);

-- state of each station at every scrape (~every 10 minutes)
-- bikes_available: total bikes physically at the station
-- bikes_available_to_rent: subset users can actually rent
CREATE TABLE station_snapshots (
    id                      BIGSERIAL PRIMARY KEY,
    station_uid             INTEGER NOT NULL REFERENCES stations(uid),
    bikes_available         INTEGER NOT NULL DEFAULT 0,
    bikes_available_to_rent INTEGER NOT NULL DEFAULT 0,
    free_racks              INTEGER NOT NULL DEFAULT 0,
    booked_bikes            INTEGER NOT NULL DEFAULT 0,
    scrape_time             TIMESTAMPTZ NOT NULL
);

-- speeds up queries like "give me station X for the last 7 days"
CREATE INDEX ON station_snapshots (station_uid, scrape_time DESC);
-- speeds up queries like "what was the whole network like at 8am on monday?"
CREATE INDEX ON station_snapshots (scrape_time DESC);

-- one row per "stay" of a bike at a station
-- derived from bikes_history: new row starts when bike changes location
-- first_seen_at: first scrape that found the bike here
-- last_seen_at: last scrape before the bike moved or disappeared
CREATE TABLE bike_positions (
    id            BIGSERIAL PRIMARY KEY,
    bike_id       TEXT NOT NULL,
    station_uid   INTEGER NOT NULL REFERENCES stations(uid),
    lat           DOUBLE PRECISION NOT NULL,
    lng           DOUBLE PRECISION NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL,
    last_seen_at  TIMESTAMPTZ NOT NULL
);

CREATE INDEX ON bike_positions (bike_id, first_seen_at DESC);
CREATE INDEX ON bike_positions (station_uid, first_seen_at DESC);

-- one row per hour for the whole city (weather is uniform across prague at this scale)
-- source: open-meteo api (free, no key required)
-- weathercode: wmo weather interpretation code (0=clear, 51-67=rain, 71-77=snow, etc.)
CREATE TABLE IF NOT EXISTS weather_hourly (
    hour          TIMESTAMPTZ NOT NULL PRIMARY KEY,
    temperature   FLOAT,
    precipitation FLOAT,
    windspeed     FLOAT,
    weathercode   SMALLINT,
    snowfall      FLOAT
);
