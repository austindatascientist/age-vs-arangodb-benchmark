#!/usr/bin/env python3
"""
Build Apache AGE graph for Alabama routing.
Creates graph nodes and relationships from shared config data.
"""

import argparse
import sys

import psycopg2

from config import INTERSECTIONS, ROADS, CITY_CONNECTIONS, PG_HOST, PG_PORT, PG_USER, PG_PASSWORD, PG_DBNAME


def escape_cypher_string(s):
    """Escape a string for use in Cypher queries."""
    if s is None:
        return ''
    return str(s).replace('\\', '\\\\').replace("'", "\\'")


def execute_cypher(cur, query):
    """Execute a Cypher query through AGE."""
    try:
        cur.execute(f"""
            SELECT * FROM ag_catalog.cypher('alabama_routing', $${query}$$) AS (result ag_catalog.agtype)
        """)
        return cur.fetchall()
    except psycopg2.Error as e:
        print(f"Cypher query failed: {e}")
        raise


def create_indexes(cur):
    """Create indexes for better query performance."""
    print("Creating indexes...")
    indexes = [
        'CREATE INDEX IF NOT EXISTS idx_city_id ON alabama_routing."City" USING BTREE (id)',
        'CREATE INDEX IF NOT EXISTS idx_city_props ON alabama_routing."City" USING GIN (properties)',
        'CREATE INDEX IF NOT EXISTS idx_intersection_id ON alabama_routing."Intersection" USING BTREE (id)',
        'CREATE INDEX IF NOT EXISTS idx_road_id ON alabama_routing."ROAD" USING BTREE (id)',
        'CREATE INDEX IF NOT EXISTS idx_road_start ON alabama_routing."ROAD" USING BTREE (start_id)',
        'CREATE INDEX IF NOT EXISTS idx_road_end ON alabama_routing."ROAD" USING BTREE (end_id)',
        'CREATE INDEX IF NOT EXISTS idx_road_start_end ON alabama_routing."ROAD" USING BTREE (start_id, end_id)',
        'CREATE INDEX IF NOT EXISTS idx_road_props ON alabama_routing."ROAD" USING GIN (properties)',
        'CREATE INDEX IF NOT EXISTS idx_nearest_start ON alabama_routing."NEAREST_INTERSECTION" USING BTREE (start_id)',
        'CREATE INDEX IF NOT EXISTS idx_nearest_end ON alabama_routing."NEAREST_INTERSECTION" USING BTREE (end_id)',
    ]
    for idx_sql in indexes:
        try:
            cur.execute(idx_sql)
        except Exception:
            pass  # Index might already exist
    print("  Indexes created")


def build_graph(cur):
    """Build graph from config data."""

    # 1. Create City nodes from database
    print("Creating City nodes...")
    cur.execute("""
        SELECT c.id, c.name, c.lat, c.lon, c.population,
               COALESCE(ta.count, 0) as tourist_attractions_count
        FROM cities c
        LEFT JOIN tourist_attractions_count ta ON c.id = ta.city_id
    """)
    cities = cur.fetchall()

    for city_id, name, lat, lon, population, tourist_attractions_count in cities:
        safe_name = escape_cypher_string(name)
        execute_cypher(cur, f"""
            CREATE (:City {{
                id: {int(city_id)},
                name: '{safe_name}',
                lat: {float(lat)},
                lon: {float(lon)},
                population: {int(population)},
                tourist_attractions_count: {int(tourist_attractions_count)}
            }})
        """)
    print(f"  Created {len(cities)} City nodes")

    # 2. Create Intersection nodes
    print("Creating Intersection nodes...")
    for osm_id, lat, lon in INTERSECTIONS:
        execute_cypher(cur, f"""
            CREATE (:Intersection {{osm_id: {int(osm_id)}, lat: {float(lat)}, lon: {float(lon)}}})
        """)
    print(f"  Created {len(INTERSECTIONS)} Intersection nodes")

    # 3. Create ROAD relationships (bidirectional)
    print("Creating ROAD relationships...")
    for start, end, name, htype, length_miles, speed_mph, travel_s in ROADS:
        safe_name = escape_cypher_string(name)
        safe_htype = escape_cypher_string(htype)
        for s, e in [(start, end), (end, start)]:
            execute_cypher(cur, f"""
                MATCH (a:Intersection {{osm_id: {int(s)}}}), (b:Intersection {{osm_id: {int(e)}}})
                CREATE (a)-[:ROAD {{
                    way_id: {int(s) * 1000 + int(e)},
                    name: '{safe_name}',
                    highway_type: '{safe_htype}',
                    length_miles: {float(length_miles)},
                    speed_mph: {int(speed_mph)},
                    travel_time_s: {float(travel_s)}
                }}]->(b)
            """)
    print(f"  Created {len(ROADS) * 2} ROAD relationships")

    # 4. Connect cities to nearest intersections
    print("Connecting cities to road network...")
    for city_name, node_id, dist_miles in CITY_CONNECTIONS:
        safe_city_name = escape_cypher_string(city_name)
        execute_cypher(cur, f"""
            MATCH (c:City {{name: '{safe_city_name}'}}), (i:Intersection {{osm_id: {int(node_id)}}})
            CREATE (c)-[:NEAREST_INTERSECTION {{distance_miles: {float(dist_miles)}}}]->(i)
        """)
        print(f"  Connected {city_name}")


def main():
    parser = argparse.ArgumentParser(description='Build AGE graph')
    parser.add_argument('--host', default=PG_HOST)
    parser.add_argument('--port', type=int, default=PG_PORT)
    parser.add_argument('--dbname', default=PG_DBNAME)
    parser.add_argument('--user', default=PG_USER)
    parser.add_argument('--password', default=PG_PASSWORD)

    args = parser.parse_args()

    print("Connecting to PostgreSQL...")
    try:
        conn = psycopg2.connect(
            host=args.host, port=args.port, dbname=args.dbname,
            user=args.user, password=args.password
        )
        conn.autocommit = True
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)

    cur = conn.cursor()

    try:
        cur.execute("LOAD 'age'")
        cur.execute("SET search_path = ag_catalog, '$user', public")

        # AGE has known performance issues with PostgreSQL JIT compilation
        cur.execute("ALTER DATABASE alabama_osm SET jit = off")

        # Drop and recreate graph
        cur.execute("SELECT * FROM ag_catalog.ag_graph WHERE name = 'alabama_routing'")
        if cur.fetchone():
            print("Dropping existing graph...")
            cur.execute("SELECT drop_graph('alabama_routing', true)")

        print("Creating graph...")
        cur.execute("SELECT create_graph('alabama_routing')")

        build_graph(cur)
        create_indexes(cur)

        # Summary
        print("\nGraph Summary:")
        cur.execute("""
            SELECT * FROM ag_catalog.cypher('alabama_routing', $$
                MATCH (n) RETURN labels(n)[0] AS label, count(n) AS cnt
            $$) AS (label ag_catalog.agtype, cnt ag_catalog.agtype)
        """)
        for row in cur.fetchall():
            print(f"  {row}")
        cur.execute("""
            SELECT * FROM ag_catalog.cypher('alabama_routing', $$
                MATCH ()-[r]->() RETURN type(r) AS rel_type, count(r) AS cnt
            $$) AS (rel_type ag_catalog.agtype, cnt ag_catalog.agtype)
        """)
        for row in cur.fetchall():
            print(f"  {row}")

    except psycopg2.Error as e:
        print(f"Database error: {e}")
        sys.exit(1)
    finally:
        cur.close()
        conn.close()

    print("\nDone!")


if __name__ == '__main__':
    main()
