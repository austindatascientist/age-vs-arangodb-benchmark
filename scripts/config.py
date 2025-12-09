#!/usr/bin/env python3
"""
Shared configuration for OSM Graph Benchmark scripts.
Contains graph data, database defaults, and common utilities.
"""

# Database defaults
PG_HOST = 'localhost'
PG_PORT = 5432
PG_USER = 'osm'
PG_PASSWORD = 'osm_password'
PG_DBNAME = 'alabama_osm'

ARANGO_HOST = 'localhost'
ARANGO_PORT = 8529
ARANGO_PASSWORD = 'osm_password'

# Graph data: Intersections along major Alabama routes
# (osm_id, lat, lon)
INTERSECTIONS = [
    (1001, 34.730, -86.586),  # Huntsville area (I-65 junction)
    (1002, 34.606, -86.983),  # Decatur (I-65)
    (1003, 33.520, -86.802),  # Birmingham area (I-65/I-20 junction)
    (1004, 32.377, -86.300),  # Montgomery area (I-65)
    (1005, 30.694, -88.043),  # Mobile area (I-65 terminus)
    (1006, 32.835, -86.629),  # Clanton (US-31, between Birmingham and Montgomery)
    (1007, 33.173, -86.251),  # Sylacauga (US-280/231, east of Birmingham)
    (1008, 32.944, -85.954),  # Alexander City (US-280/231)
]

# Graph data: Road segments
# (start_id, end_id, name, highway_type, length_miles, speed_mph, travel_time_s)
ROADS = [
    # I-65 corridor
    (1001, 1002, 'I-65', 'motorway', 25, 70, 1286),   # Huntsville -> Decatur
    (1002, 1003, 'I-65', 'motorway', 81, 70, 4166),   # Decatur -> Birmingham
    (1003, 1004, 'I-65', 'motorway', 90, 70, 4629),   # Birmingham -> Montgomery
    (1004, 1005, 'I-65', 'motorway', 168, 70, 8640),  # Montgomery -> Mobile
    # US-31 route
    (1003, 1006, 'US-31', 'primary', 40, 55, 2618),   # Birmingham -> Clanton
    (1006, 1004, 'US-31', 'primary', 53, 55, 3469),   # Clanton -> Montgomery
    # US-280/231 route
    (1003, 1007, 'US-280', 'primary', 40, 55, 2618),  # Birmingham -> Sylacauga
    (1007, 1008, 'US-280', 'primary', 25, 55, 1636),  # Sylacauga -> Alexander City
    (1008, 1004, 'US-231', 'primary', 50, 55, 3273),  # Alexander City -> Montgomery
]

# Graph data: City to intersection connections
# (city_name, intersection_id, distance_miles)
CITY_CONNECTIONS = [
    ('Huntsville', 1001, 3),
    ('Birmingham', 1003, 2),
    ('Montgomery', 1004, 1),
    ('Mobile', 1005, 2),
    ('Tuscaloosa', 1003, 50),  # Connected via Birmingham junction
]


def add_pg_args(parser):
    """Add PostgreSQL connection arguments to an argument parser."""
    parser.add_argument('--pg-host', default=PG_HOST)
    parser.add_argument('--pg-port', type=int, default=PG_PORT)
    parser.add_argument('--pg-user', default=PG_USER)
    parser.add_argument('--pg-password', default=PG_PASSWORD)
    parser.add_argument('--dbname', default=PG_DBNAME)


def add_arango_args(parser):
    """Add ArangoDB connection arguments to an argument parser."""
    parser.add_argument('--arango-host', default=ARANGO_HOST)
    parser.add_argument('--arango-port', type=int, default=ARANGO_PORT)
    parser.add_argument('--arango-password', default=ARANGO_PASSWORD)


def intersection_key(osm_id):
    """Generate a consistent key for an intersection (used by ArangoDB)."""
    return f'int_{osm_id}'
