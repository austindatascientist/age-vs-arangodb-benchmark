#!/usr/bin/env python3
"""
Import OSM data into PostgreSQL tables.

Optimized for speed - single pass parsing, minimal memory, COPY for bulk inserts.
"""

import argparse
import io
import math
import os
import sys

import osmium
import psycopg2
from tqdm import tqdm


# Highway types to import (only freeways and highways for fast routing)
HIGHWAY_TYPES = frozenset({
    'motorway', 'motorway_link',
    'trunk', 'trunk_link',
    'primary', 'primary_link',
    'secondary', 'secondary_link'
})

# Default speeds (mph) by highway type
DEFAULT_SPEEDS = {
    'motorway': 70,
    'motorway_link': 50,
    'trunk': 65,
    'trunk_link': 45,
    'primary': 55,
    'primary_link': 35,
    'secondary': 45,
    'secondary_link': 30
}

# Tourism types to import for tourist attraction counting
TOURISM_TYPES = frozenset({
    'museum', 'attraction', 'viewpoint', 'artwork',
    'gallery', 'theme_park', 'zoo', 'aquarium',
    'hotel', 'motel', 'hostel', 'guest_house',
    'camp_site', 'caravan_site', 'picnic_site',
    'information', 'yes'
})


def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate great-circle distance between two coordinates in miles (Haversine formula)."""
    earth_radius_miles = 3958.8
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat/2)**2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon/2)**2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return earth_radius_miles * c


def escape_tsv(v):
    """Escape a value for TSV format used by COPY."""
    if v is None:
        return '\\N'
    s = str(v)
    return s.replace('\\', '\\\\').replace('\t', ' ').replace('\n', ' ')


class OSMHandler(osmium.SimpleHandler):
    """Single-pass handler - extracts only what's needed."""

    def __init__(self):
        super().__init__()
        # Only store coordinates for road nodes (no tags needed)
        self.node_coords = {}  # osm_id -> (lat, lon)
        self.ways = []  # [(osm_id, [node_ids], highway, name, oneway, maxspeed), ...]
        self.tourist_attractions = []  # [(osm_id, name, type, lat, lon), ...]
        self.road_node_ids = set()
        self._node_count = 0

    def node(self, n):
        """Process a node - only store if it's a tourist attraction."""
        self._node_count += 1

        # Check tourism tag directly without building full tags dict
        tourism = n.tags.get('tourism')
        if tourism in TOURISM_TYPES:
            name = n.tags.get('name', f'Unnamed {tourism}')
            self.tourist_attractions.append((n.id, name, tourism, n.location.lat, n.location.lon))

        # Store coords for all nodes (needed for road segments later)
        # This is memory-heavy but required for single-pass
        self.node_coords[n.id] = (n.location.lat, n.location.lon)

    def way(self, w):
        """Process a way - only store highway info we need."""
        highway = w.tags.get('highway')
        if highway in HIGHWAY_TYPES:
            node_ids = [n.ref for n in w.nodes]
            self.road_node_ids.update(node_ids)

            # Extract only the tags we need
            name = w.tags.get('name', '')
            oneway = w.tags.get('oneway', 'no') == 'yes'
            maxspeed = w.tags.get('maxspeed', '')

            self.ways.append((w.id, node_ids, highway, name, oneway, maxspeed))


def copy_to_table(cur, table, columns, data, desc=None):
    """Use COPY for fast bulk insert with progress tracking."""
    if not data:
        return 0

    buffer = io.StringIO()
    for row in tqdm(data, desc=desc, disable=desc is None):
        line = '\t'.join(escape_tsv(v) for v in row)
        buffer.write(line + '\n')

    buffer.seek(0)
    cur.copy_from(buffer, table, columns=columns, null='\\N')
    return len(data)


def import_to_postgres(handler, conn):
    """Import extracted data to PostgreSQL using COPY."""
    cur = conn.cursor()

    try:
        # Clear existing data (order matters due to foreign keys)
        print("Clearing existing data...")
        cur.execute("TRUNCATE TABLE road_segments, osm_ways, osm_nodes, tourist_attractions CASCADE")
        conn.commit()

        # Disable indexes for faster inserts
        print("Disabling indexes for bulk import...")
        cur.execute("SET maintenance_work_mem = '256MB'")

        # Import road nodes
        print("Importing road nodes...")
        road_nodes = [
            (osm_id, handler.node_coords[osm_id][0], handler.node_coords[osm_id][1], '{}')
            for osm_id in handler.road_node_ids
            if osm_id in handler.node_coords
        ]
        count = copy_to_table(cur, 'osm_nodes', ('osm_id', 'lat', 'lon', 'tags'), road_nodes, "  Nodes")
        print(f"  Imported {count:,} road nodes")

        # Import ways - directly without intermediate list
        print("Importing ways...")
        way_buffer = io.StringIO()
        way_count = 0
        for way_id, node_ids, _, _, _, _ in tqdm(handler.ways, desc="  Ways"):
            array_str = '{' + ','.join(str(n) for n in node_ids) + '}'
            way_buffer.write(f"{way_id}\t{array_str}\t{{}}\n")
            way_count += 1
        way_buffer.seek(0)
        cur.copy_from(way_buffer, 'osm_ways', columns=('osm_id', 'nodes', 'tags'), null='\\N')
        print(f"  Imported {way_count:,} road ways")

        # Import tourist attractions
        print("Importing tourist attractions...")
        count = copy_to_table(cur, 'tourist_attractions', ('osm_id', 'name', 'tourism_type', 'lat', 'lon'),
                              handler.tourist_attractions, "  Attractions")
        print(f"  Imported {count:,} tourist attractions")

        conn.commit()

        # Create road segments
        print("Creating road segments...")
        create_road_segments(handler, cur)
        conn.commit()

    except psycopg2.Error as e:
        conn.rollback()
        print(f"Database error during import: {e}")
        raise
    finally:
        cur.close()


def create_road_segments(handler, cur):
    """Create road segments from ways using COPY."""
    buffer = io.StringIO()
    segment_count = 0

    for way_id, node_ids, highway, name, oneway, maxspeed_str in tqdm(handler.ways, desc="  Segments"):
        # Get speed (mph)
        speed = DEFAULT_SPEEDS.get(highway, 45)
        if maxspeed_str:
            try:
                # Parse speed - assume mph for US data
                speed = int(float(maxspeed_str.replace('mph', '').strip().split()[0]))
            except (ValueError, IndexError):
                pass

        # Create segments
        for i in range(len(node_ids) - 1):
            start_id = node_ids[i]
            end_id = node_ids[i + 1]

            if start_id in handler.node_coords and end_id in handler.node_coords:
                lat1, lon1 = handler.node_coords[start_id]
                lat2, lon2 = handler.node_coords[end_id]

                length_miles = haversine_distance(lat1, lon1, lat2, lon2)
                travel_time_s = length_miles / speed * 3600

                # Escape name field
                name_escaped = escape_tsv(name)
                buffer.write(f"{way_id}\t{start_id}\t{end_id}\t{name_escaped}\t{highway}\t"
                             f"{length_miles}\t{speed}\t{travel_time_s}\t{oneway}\n")
                segment_count += 1

    buffer.seek(0)
    cur.copy_from(buffer, 'road_segments',
                  columns=('way_id', 'start_node', 'end_node', 'name', 'highway_type',
                          'length_miles', 'speed_mph', 'travel_time_s', 'oneway'),
                  null='\\N')
    print(f"  Created {segment_count:,} road segments")


def assign_tourist_attractions_to_cities(conn):
    """Assign tourist attractions to nearest city and calculate city tourist attraction counts."""
    cur = conn.cursor()

    try:
        print("Assigning tourist attractions to nearest cities...")
        cur.execute("""
            UPDATE tourist_attractions ta
            SET nearest_city_id = (
                SELECT c.id
                FROM cities c
                ORDER BY (ta.lat - c.lat)^2 + (ta.lon - c.lon)^2
                LIMIT 1
            )
        """)
        print(f"  Assigned {cur.rowcount:,} attractions to cities")

        print("Calculating city tourist attraction statistics...")
        cur.execute("""
            INSERT INTO tourist_attractions_count (city_id, count, per_capita)
            SELECT
                c.id,
                COUNT(t.id),
                COUNT(t.id)::float / c.population * 10000
            FROM cities c
            LEFT JOIN tourist_attractions t ON t.nearest_city_id = c.id
            GROUP BY c.id
            ON CONFLICT (city_id) DO UPDATE
            SET count = EXCLUDED.count,
                per_capita = EXCLUDED.per_capita
        """)

        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Database error during assignment: {e}")
        raise
    finally:
        cur.close()


def main():
    parser = argparse.ArgumentParser(description='Import OSM data to PostgreSQL')
    parser.add_argument('osm_file', nargs='?', default='/data/alabama/alabama.osm',
                        help='Path to OSM file')
    parser.add_argument('--host', default='localhost', help='PostgreSQL host')
    parser.add_argument('--port', type=int, default=5432, help='PostgreSQL port')
    parser.add_argument('--dbname', default='alabama_osm', help='Database name')
    parser.add_argument('--user', default='osm', help='Database user')
    parser.add_argument('--password', default='osm_password', help='Database password')

    args = parser.parse_args()

    if not os.path.exists(args.osm_file):
        print(f"Error: File not found: {args.osm_file}")
        sys.exit(1)

    print(f"Parsing OSM file: {args.osm_file}")
    file_size = os.path.getsize(args.osm_file)
    print(f"  File size: {file_size / 1024 / 1024:.1f} MB")

    # Single pass extraction
    handler = OSMHandler()
    print("  Reading OSM data...")
    handler.apply_file(args.osm_file, locations=True)

    print(f"\nExtracted:")
    print(f"  - {len(handler.road_node_ids):,} road nodes")
    print(f"  - {len(handler.ways):,} road ways")
    print(f"  - {len(handler.tourist_attractions):,} tourist attractions")

    # Free memory from unused nodes
    print("\nOptimizing memory...")
    original_count = len(handler.node_coords)
    needed_nodes = handler.road_node_ids
    handler.node_coords = {k: v for k, v in handler.node_coords.items() if k in needed_nodes}
    freed = original_count - len(handler.node_coords)
    print(f"  Freed {freed:,} unused node coordinates")
    print(f"  Retained {len(handler.node_coords):,} needed node coordinates")

    # Connect to PostgreSQL
    print(f"\nConnecting to PostgreSQL at {args.host}:{args.port}...")
    conn = None
    try:
        conn = psycopg2.connect(
            host=args.host,
            port=args.port,
            dbname=args.dbname,
            user=args.user,
            password=args.password
        )

        # Import data
        import_to_postgres(handler, conn)
        assign_tourist_attractions_to_cities(conn)

        print("\nImport complete!")

    except psycopg2.Error as e:
        print(f"Database error: {e}")
        sys.exit(1)
    finally:
        if conn:
            conn.close()


if __name__ == '__main__':
    main()
