#!/usr/bin/env python3
"""
Build ArangoDB graph for Alabama routing.
Creates graph data matching the AGE graph using shared config.
"""

import argparse
import sys

import psycopg2
from arango import ArangoClient

from config import (
    INTERSECTIONS, ROADS, CITY_CONNECTIONS,
    PG_HOST, PG_PORT, PG_USER, PG_PASSWORD, PG_DBNAME,
    ARANGO_HOST, ARANGO_PORT, ARANGO_PASSWORD,
    intersection_key
)


def setup_database(client, db_name, password):
    """Create database, collections, and graph."""
    sys_db = client.db('_system', username='root', password=password)

    if not sys_db.has_database(db_name):
        sys_db.create_database(db_name)
        print(f"Created database: {db_name}")

    db = client.db(db_name, username='root', password=password)

    # Create document collections
    for name in ['cities', 'intersections']:
        if not db.has_collection(name):
            db.create_collection(name)
            print(f"Created collection: {name}")

    # Create edge collections
    for name in ['roads', 'nearest_intersection']:
        if not db.has_collection(name):
            db.create_collection(name, edge=True)
            print(f"Created edge collection: {name}")

    # Create graph
    if not db.has_graph('alabama_routing'):
        db.create_graph(
            'alabama_routing',
            edge_definitions=[
                {
                    'edge_collection': 'roads',
                    'from_vertex_collections': ['intersections'],
                    'to_vertex_collections': ['intersections']
                },
                {
                    'edge_collection': 'nearest_intersection',
                    'from_vertex_collections': ['cities'],
                    'to_vertex_collections': ['intersections']
                }
            ]
        )
        print("Created graph: alabama_routing")

    # Create indexes
    cities = db.collection('cities')
    cities.add_index({'type': 'persistent', 'fields': ['name'], 'unique': True})
    cities.add_index({'type': 'persistent', 'fields': ['population']})

    intersections = db.collection('intersections')
    intersections.add_index({'type': 'geo', 'fields': ['location']})
    intersections.add_index({'type': 'persistent', 'fields': ['osm_id'], 'unique': True})

    roads = db.collection('roads')
    roads.add_index({'type': 'persistent', 'fields': ['travel_time_s']})
    roads.add_index({'type': 'persistent', 'fields': ['length_miles']})

    print("Created indexes")

    return db


def build_graph(db, pg_conn):
    """Build graph from config data."""

    cities_col = db.collection('cities')
    intersections_col = db.collection('intersections')
    roads_col = db.collection('roads')
    nearest_col = db.collection('nearest_intersection')

    # Clear existing data
    cities_col.truncate()
    intersections_col.truncate()
    roads_col.truncate()
    nearest_col.truncate()
    print("Cleared existing data")

    # 1. Create City documents from PostgreSQL
    print("Creating City documents...")
    cur = pg_conn.cursor()
    cur.execute("""
        SELECT c.id, c.name, c.lat, c.lon, c.population, c.county,
               COALESCE(ta.count, 0) as tourist_attractions_count
        FROM cities c
        LEFT JOIN tourist_attractions_count ta ON c.id = ta.city_id
    """)
    cities = cur.fetchall()

    city_docs = []
    for city_id, name, lat, lon, population, county, tourist_attractions_count in cities:
        key = name.lower().replace(' ', '_')
        city_docs.append({
            '_key': key,
            'name': name,
            'lat': float(lat),
            'lon': float(lon),
            'location': [float(lat), float(lon)],
            'population': int(population),
            'county': county,
            'tourist_attractions_count': int(tourist_attractions_count)
        })
    cities_col.insert_many(city_docs)
    print(f"  Created {len(city_docs)} City documents")
    cur.close()

    # 2. Create Intersection documents
    print("Creating Intersection documents...")
    int_docs = []
    for osm_id, lat, lon in INTERSECTIONS:
        int_docs.append({
            '_key': intersection_key(osm_id),
            'osm_id': osm_id,
            'lat': lat,
            'lon': lon,
            'location': [lat, lon]
        })
    intersections_col.insert_many(int_docs)
    print(f"  Created {len(int_docs)} Intersection documents")

    # 3. Create ROAD edges (bidirectional)
    print("Creating ROAD edges...")
    road_edges = []
    for start, end, name, htype, length_miles, speed_mph, travel_s in ROADS:
        for s, e in [(start, end), (end, start)]:
            road_edges.append({
                '_from': f'intersections/{intersection_key(s)}',
                '_to': f'intersections/{intersection_key(e)}',
                'way_id': s * 1000 + e,
                'name': name,
                'highway_type': htype,
                'length_miles': float(length_miles),
                'speed_mph': speed_mph,
                'travel_time_s': float(travel_s)
            })
    roads_col.insert_many(road_edges)
    print(f"  Created {len(road_edges)} ROAD edges")

    # 4. Connect cities to nearest intersections
    print("Connecting cities to road network...")
    conn_edges = []
    for city_name, node_id, dist_miles in CITY_CONNECTIONS:
        city_key = city_name.lower().replace(' ', '_')
        conn_edges.append({
            '_from': f'cities/{city_key}',
            '_to': f'intersections/{intersection_key(node_id)}',
            'distance_miles': float(dist_miles)
        })
        print(f"  Connected {city_name}")
    nearest_col.insert_many(conn_edges)


def print_summary(db):
    """Print graph summary."""
    print("\nGraph Summary:")
    for name in ['cities', 'intersections', 'roads', 'nearest_intersection']:
        col = db.collection(name)
        print(f"  {name}: {col.count()} documents")


def main():
    parser = argparse.ArgumentParser(description='Build ArangoDB graph')
    parser.add_argument('--arango-host', default=ARANGO_HOST)
    parser.add_argument('--arango-port', type=int, default=ARANGO_PORT)
    parser.add_argument('--arango-password', default=ARANGO_PASSWORD)
    parser.add_argument('--dbname', default=PG_DBNAME)
    parser.add_argument('--pg-host', default=PG_HOST)
    parser.add_argument('--pg-port', type=int, default=PG_PORT)
    parser.add_argument('--pg-user', default=PG_USER)
    parser.add_argument('--pg-password', default=PG_PASSWORD)

    args = parser.parse_args()

    # Connect to PostgreSQL (to read city data)
    print("Connecting to PostgreSQL...")
    try:
        pg_conn = psycopg2.connect(
            host=args.pg_host, port=args.pg_port, dbname=args.dbname,
            user=args.pg_user, password=args.pg_password
        )
    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL: {e}")
        sys.exit(1)

    # Connect to ArangoDB
    print("Connecting to ArangoDB...")
    try:
        client = ArangoClient(hosts=f'http://{args.arango_host}:{args.arango_port}')
        db = setup_database(client, args.dbname, args.arango_password)
    except Exception as e:
        print(f"Error connecting to ArangoDB: {e}")
        sys.exit(1)

    try:
        build_graph(db, pg_conn)
        print_summary(db)
    except Exception as e:
        print(f"Error building graph: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        pg_conn.close()

    print("\nDone!")


if __name__ == '__main__':
    main()
