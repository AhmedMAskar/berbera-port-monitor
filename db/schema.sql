-- Enable PostGIS
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS geofences (
  id   text PRIMARY KEY,
  geom geometry(Polygon, 4326) NOT NULL
);

CREATE TABLE IF NOT EXISTS ais_positions (
  id           bigserial PRIMARY KEY,
  mmsi         bigint NOT NULL,
  received_at  timestamptz NOT NULL,
  lat          double precision,
  lon          double precision,
  sog          double precision,
  cog          double precision,
  nav_status   text,
  destination  text,
  geom         geometry(Point, 4326)
);
CREATE INDEX IF NOT EXISTS idx_ais_mmsi_time ON ais_positions (mmsi, received_at DESC);
CREATE INDEX IF NOT EXISTS idx_ais_geom ON ais_positions USING GIST (geom);

CREATE TABLE IF NOT EXISTS port_calls (
  id              bigserial PRIMARY KEY,
  mmsi            bigint NOT NULL,
  arrival_at      timestamptz NOT NULL,
  departure_at    timestamptz,
  waiting_minutes int DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_calls_arr ON port_calls (arrival_at);
CREATE INDEX IF NOT EXISTS idx_calls_dep ON port_calls (departure_at);
