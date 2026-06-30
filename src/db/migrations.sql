-- Enable the spatial engine extension
CREATE EXTENSION IF NOT EXISTS postgis;

-- Create the Port/AOI Boundary table
CREATE TABLE IF NOT EXISTS port_geometries (
port_id SERIAL PRIMARY KEY,
port_name VARCHAR(100) NOT NULL,
commodity_type VARCHAR(50), -- e.g., 'Crude Oil', 'Grains'
geom GEOMETRY(Polygon, 4326) NOT NULL
);

-- Create the heavy telemetry table
CREATE TABLE IF NOT EXISTS vessel_telemetry (
telemetry_id BIGSERIAL PRIMARY KEY,
mmsi INT NOT NULL,
timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
sog REAL,
geom GEOMETRY(Point, 4326) NOT NULL
);

-- THE HEDGE FUND FLEX: Build spatial R-Tree indexes to optimize bounding-box queries
CREATE INDEX IF NOT EXISTS idx_port_spatial_geom ON port_geometries USING gist(geom);
CREATE INDEX IF NOT EXISTS idx_vessel_spatial_geom ON vessel_telemetry USING gist(geom);

-- Composite temporal index for rapid time-series analysis
CREATE INDEX IF NOT EXISTS idx_vessel_mmsi_time ON vessel_telemetry(mmsi, timestamp DESC);
