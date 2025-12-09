-- Schema for Alabama OSM data

-- Cities table with Census population data
CREATE TABLE IF NOT EXISTS cities (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    lat DOUBLE PRECISION NOT NULL,
    lon DOUBLE PRECISION NOT NULL,
    population INTEGER NOT NULL,
    county VARCHAR(100)
);

-- Raw OSM nodes (road network nodes)
CREATE TABLE IF NOT EXISTS osm_nodes (
    osm_id BIGINT PRIMARY KEY,
    lat DOUBLE PRECISION NOT NULL,
    lon DOUBLE PRECISION NOT NULL,
    tags JSONB DEFAULT '{}'::jsonb
);

-- Raw OSM ways (roads)
CREATE TABLE IF NOT EXISTS osm_ways (
    osm_id BIGINT PRIMARY KEY,
    nodes BIGINT[] NOT NULL,
    tags JSONB DEFAULT '{}'::jsonb
);

-- Processed road segments for routing
CREATE TABLE IF NOT EXISTS road_segments (
    id SERIAL PRIMARY KEY,
    way_id BIGINT NOT NULL,
    start_node BIGINT NOT NULL,
    end_node BIGINT NOT NULL,
    name VARCHAR(255),
    highway_type VARCHAR(50) NOT NULL,
    length_miles DOUBLE PRECISION NOT NULL,
    speed_mph INTEGER NOT NULL,
    travel_time_s DOUBLE PRECISION NOT NULL,
    oneway BOOLEAN DEFAULT FALSE
);

-- Tourist attractions from OSM tourism data
CREATE TABLE IF NOT EXISTS tourist_attractions (
    id SERIAL PRIMARY KEY,
    osm_id BIGINT NOT NULL,
    name VARCHAR(255),
    tourism_type VARCHAR(100) NOT NULL,
    lat DOUBLE PRECISION NOT NULL,
    lon DOUBLE PRECISION NOT NULL,
    nearest_city_id INTEGER REFERENCES cities(id)
);

-- Aggregated tourist attraction counts per city
CREATE TABLE IF NOT EXISTS tourist_attractions_count (
    city_id INTEGER PRIMARY KEY REFERENCES cities(id),
    count INTEGER DEFAULT 0,
    per_capita DOUBLE PRECISION DEFAULT 0.0
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_osm_nodes_coords ON osm_nodes(lat, lon);
CREATE INDEX IF NOT EXISTS idx_osm_ways_tags ON osm_ways USING GIN(tags);
CREATE INDEX IF NOT EXISTS idx_road_segments_nodes ON road_segments(start_node, end_node);
CREATE INDEX IF NOT EXISTS idx_road_segments_highway ON road_segments(highway_type);
CREATE INDEX IF NOT EXISTS idx_road_segments_way_id ON road_segments(way_id);
CREATE INDEX IF NOT EXISTS idx_tourist_attractions_type ON tourist_attractions(tourism_type);
CREATE INDEX IF NOT EXISTS idx_tourist_attractions_coords ON tourist_attractions(lat, lon);
CREATE INDEX IF NOT EXISTS idx_tourist_attractions_city_id ON tourist_attractions(nearest_city_id);
