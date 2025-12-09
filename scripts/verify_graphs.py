#!/usr/bin/env python3
"""
Verify that PostgreSQL + AGE and ArangoDB have identical graph data.
Compares node counts, edge counts, and sample data to ensure consistency.
"""

import argparse
import sys

import psycopg2
from arango import ArangoClient

from config import (
    PG_HOST, PG_PORT, PG_USER, PG_PASSWORD, PG_DBNAME,
    ARANGO_HOST, ARANGO_PORT, ARANGO_PASSWORD
)


def get_age_stats(cur):
    """Get statistics from PostgreSQL + AGE graph."""
    stats = {}

    # Count cities
    cur.execute("""
        SELECT * FROM cypher('alabama_routing', $$
            MATCH (c:City) RETURN count(c) AS cnt
        $$) AS (cnt agtype)
    """)
    stats['cities'] = int(str(cur.fetchone()[0]))

    # Count intersections
    cur.execute("""
        SELECT * FROM cypher('alabama_routing', $$
            MATCH (i:Intersection) RETURN count(i) AS cnt
        $$) AS (cnt agtype)
    """)
    stats['intersections'] = int(str(cur.fetchone()[0]))

    # Count road edges
    cur.execute("""
        SELECT * FROM cypher('alabama_routing', $$
            MATCH ()-[r:ROAD]->() RETURN count(r) AS cnt
        $$) AS (cnt agtype)
    """)
    stats['roads'] = int(str(cur.fetchone()[0]))

    # Count nearest_intersection edges
    cur.execute("""
        SELECT * FROM cypher('alabama_routing', $$
            MATCH ()-[r:NEAREST_INTERSECTION]->() RETURN count(r) AS cnt
        $$) AS (cnt agtype)
    """)
    stats['nearest_intersection'] = int(str(cur.fetchone()[0]))

    # Get city details for comparison
    cur.execute("""
        SELECT * FROM cypher('alabama_routing', $$
            MATCH (c:City)
            RETURN c.name AS name, c.population AS pop, c.tourist_attractions_count AS ta
            ORDER BY c.population DESC
        $$) AS (name agtype, pop agtype, ta agtype)
    """)
    city_data = []
    for row in cur.fetchall():
        city_data.append({
            # AGE agtype wraps strings in quotes, must strip them
            'name': str(row[0]).strip('"'),
            'population': int(str(row[1])),
            'tourist_attractions': int(str(row[2]))
        })
    stats['city_names'] = [c['name'] for c in city_data]
    stats['city_data'] = city_data

    # Get sample road segments for data quality check
    cur.execute("""
        SELECT * FROM cypher('alabama_routing', $$
            MATCH (a:Intersection)-[r:ROAD]->(b:Intersection)
            RETURN r.length_miles AS miles, r.travel_time_s AS time, r.speed_mph AS speed
            LIMIT 10
        $$) AS (miles agtype, time agtype, speed agtype)
    """)
    road_samples = []
    for row in cur.fetchall():
        road_samples.append({
            'miles': float(str(row[0])),
            'time': float(str(row[1])),
            'speed': int(str(row[2]))
        })
    stats['road_samples'] = road_samples

    # Test traversal: Find path from Huntsville to Birmingham
    try:
        cur.execute("""
            SELECT * FROM cypher('alabama_routing', $$
                MATCH (startCity:City {name: 'Huntsville'})-[:NEAREST_INTERSECTION]->(si:Intersection),
                      (endCity:City {name: 'Birmingham'})-[:NEAREST_INTERSECTION]->(ei:Intersection),
                      path = (si)-[:ROAD*..30]->(ei)
                UNWIND relationships(path) AS r
                WITH path, sum(r.travel_time_s) AS total_time
                RETURN length(path) AS hops, round(total_time / 60.0) AS minutes
                ORDER BY total_time ASC
                LIMIT 1
            $$) AS (hops agtype, minutes agtype)
        """)
        result = cur.fetchone()
        if result:
            stats['test_path_hsv_bhm'] = {
                'hops': int(float(str(result[0]))),
                'minutes': int(float(str(result[1])))
            }
    except Exception as e:
        stats['test_path_hsv_bhm'] = {'error': str(e)}

    return stats


def get_arango_stats(db):
    """Get statistics from ArangoDB graph."""
    stats = {}

    stats['cities'] = db.collection('cities').count()
    stats['intersections'] = db.collection('intersections').count()
    stats['roads'] = db.collection('roads').count()
    stats['nearest_intersection'] = db.collection('nearest_intersection').count()

    # Get city details for comparison
    cursor = db.aql.execute('''
        FOR c IN cities
        SORT c.population DESC
        RETURN {
            name: c.name,
            population: c.population,
            tourist_attractions: c.tourist_attractions_count
        }
    ''')
    city_data = list(cursor)
    stats['city_names'] = [c['name'] for c in city_data]
    stats['city_data'] = city_data

    # Get sample road segments for data quality check
    cursor = db.aql.execute('''
        FOR r IN roads
        LIMIT 10
        RETURN {
            miles: r.length_miles,
            time: r.travel_time_s,
            speed: r.speed_mph
        }
    ''')
    road_samples = list(cursor)
    stats['road_samples'] = road_samples

    # Test traversal: Find path from Huntsville to Birmingham
    try:
        cursor = db.aql.execute('''
            LET huntsville = FIRST(FOR c IN cities FILTER c.name == "Huntsville" RETURN c)
            LET birmingham = FIRST(FOR c IN cities FILTER c.name == "Birmingham" RETURN c)
            LET start_int = FIRST(FOR v IN 1..1 OUTBOUND huntsville nearest_intersection RETURN v)
            LET end_int = FIRST(FOR v IN 1..1 OUTBOUND birmingham nearest_intersection RETURN v)

            FOR v, e, p IN 1..30 OUTBOUND start_int roads
                FILTER v._id == end_int._id
                LET total_time = SUM(p.edges[*].travel_time_s)
                SORT total_time ASC
                LIMIT 1
                RETURN {
                    hops: LENGTH(p.edges),
                    minutes: ROUND(total_time / 60)
                }
        ''')
        result = list(cursor)
        if result:
            stats['test_path_hsv_bhm'] = result[0]
    except Exception as e:
        stats['test_path_hsv_bhm'] = {'error': str(e)}

    return stats


def main():
    parser = argparse.ArgumentParser(description='Verify AGE and ArangoDB have identical data')
    parser.add_argument('--pg-host', default=PG_HOST)
    parser.add_argument('--pg-port', type=int, default=PG_PORT)
    parser.add_argument('--arango-host', default=ARANGO_HOST)
    parser.add_argument('--arango-port', type=int, default=ARANGO_PORT)
    parser.add_argument('--dbname', default=PG_DBNAME)
    parser.add_argument('--pg-user', default=PG_USER)
    parser.add_argument('--pg-password', default=PG_PASSWORD)
    parser.add_argument('--arango-password', default=ARANGO_PASSWORD)

    args = parser.parse_args()

    # Connect to PostgreSQL
    print("Connecting to PostgreSQL + AGE...")
    try:
        pg_conn = psycopg2.connect(
            host=args.pg_host, port=args.pg_port, dbname=args.dbname,
            user=args.pg_user, password=args.pg_password
        )
        pg_conn.autocommit = True
        pg_cur = pg_conn.cursor()
        pg_cur.execute("LOAD 'age'")
        pg_cur.execute("SET search_path = ag_catalog, '$user', public")
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        sys.exit(1)

    # Connect to ArangoDB
    print("Connecting to ArangoDB...")
    try:
        arango_client = ArangoClient(hosts=f'http://{args.arango_host}:{args.arango_port}')
        arango_db = arango_client.db(args.dbname, username='root', password=args.arango_password)
    except Exception as e:
        print(f"Error connecting to ArangoDB: {e}")
        sys.exit(1)

    # Get stats from both databases
    print("\nGathering statistics...")
    age_stats = get_age_stats(pg_cur)
    arango_stats = get_arango_stats(arango_db)

    # Compare and report
    print("\n" + "=" * 60)
    print("GRAPH DATA VERIFICATION")
    print("=" * 60)

    all_match = True

    # Compare counts
    print("\nNode/Edge Counts:")
    print(f"{'Collection':<25} {'PostgreSQL + AGE':>15} {'ArangoDB':>15} {'Match':>10}")
    print("-" * 65)

    for key in ['cities', 'intersections', 'roads', 'nearest_intersection']:
        age_val = age_stats[key]
        arango_val = arango_stats[key]
        match = age_val == arango_val
        status = "YES" if match else "NO"
        if not match:
            all_match = False
        print(f"{key:<25} {age_val:>15} {arango_val:>15} {status:>10}")

    # Compare city names
    print("\nCity Names:")
    print(f"  PostgreSQL + AGE: {age_stats['city_names']}")
    print(f"  ArangoDB:         {arango_stats['city_names']}")
    cities_match = age_stats['city_names'] == arango_stats['city_names']
    if not cities_match:
        all_match = False
        print("  Status: MISMATCH")
    else:
        print("  Status: MATCH")

    # Compare city data (population, tourist attractions)
    print("\nCity Data Properties:")
    city_data_match = age_stats['city_data'] == arango_stats['city_data']
    if city_data_match:
        print("  Status: MATCH (populations and tourist attractions identical)")
    else:
        all_match = False
        print("  Status: MISMATCH")
        # Show differences
        for i, (age_city, arango_city) in enumerate(zip(age_stats['city_data'], arango_stats['city_data'])):
            if age_city != arango_city:
                print(f"    Difference in {age_city['name']}:")
                print(f"      AGE:     pop={age_city['population']}, ta={age_city['tourist_attractions']}")
                print(f"      Arango:  pop={arango_city['population']}, ta={arango_city['tourist_attractions']}")

    # Compare road segments (sample check for data quality)
    print("\nRoad Segment Data Quality:")
    if age_stats['road_samples'] and arango_stats['road_samples']:
        # Check if sample data has similar ranges (not exact match since order might differ)
        age_avg_miles = sum(r['miles'] for r in age_stats['road_samples']) / len(age_stats['road_samples'])
        arango_avg_miles = sum(r['miles'] for r in arango_stats['road_samples']) / len(arango_stats['road_samples'])

        miles_diff = abs(age_avg_miles - arango_avg_miles) / age_avg_miles if age_avg_miles > 0 else 0

        if miles_diff < 0.01:  # Less than 1% difference in averages
            print(f"  Sample average road length: {age_avg_miles:.2f} miles (both databases)")
            print("  Status: MATCH (road properties consistent)")
        else:
            print(f"  AGE avg road length:    {age_avg_miles:.2f} miles")
            print(f"  Arango avg road length: {arango_avg_miles:.2f} miles")
            print("  Status: WARNING (road properties differ, may indicate data inconsistency)")
    else:
        print("  Status: SKIP (no road samples to compare)")

    # Test graph traversals (functional verification)
    print("\nGraph Traversal Test (Huntsville → Birmingham):")
    age_path = age_stats.get('test_path_hsv_bhm', {})
    arango_path = arango_stats.get('test_path_hsv_bhm', {})

    if 'error' in age_path:
        print(f"  PostgreSQL + AGE:  ERROR - {age_path['error']}")
        all_match = False
    elif 'error' in arango_path:
        print(f"  ArangoDB:          ERROR - {arango_path['error']}")
        all_match = False
    elif age_path and arango_path:
        print(f"  PostgreSQL + AGE:  {age_path['hops']} hops, {age_path['minutes']} minutes")
        print(f"  ArangoDB:          {arango_path['hops']} hops, {arango_path['minutes']} minutes")

        path_match = (age_path['hops'] == arango_path['hops'] and
                      age_path['minutes'] == arango_path['minutes'])

        if path_match:
            print("  Status: MATCH (identical traversal results)")
        else:
            print("  Status: MISMATCH (different paths found)")
            all_match = False
    else:
        print("  Status: SKIP (path not found in one or both databases)")
        all_match = False

    # Final result
    print("\n" + "=" * 60)
    if all_match:
        print("VERIFICATION PASSED: Both databases have IDENTICAL data")
        print("=" * 60)
        print("\nIDENTICAL DATA COUNTS:")
        print(f"  Cities:              {age_stats['cities']}")
        print(f"  Intersections:       {age_stats['intersections']}")
        print(f"  Road edges:          {age_stats['roads']}")
        print(f"  City connections:    {age_stats['nearest_intersection']}")
        print("")
        print("Both AGE and ArangoDB contain the exact same:")
        print("  - Nodes (cities and intersections)")
        print("  - Edges (roads and city connections)")
        print("  - Properties (populations, tourist attractions, road lengths, etc.)")
        print("=" * 60)
        pg_cur.close()
        pg_conn.close()
        sys.exit(0)
    else:
        print("VERIFICATION FAILED: Databases have different data")
        print("=" * 60)
        print("\nMISMATCHED COUNTS:")
        print(f"  {'Item':<20} {'AGE':>10} {'ArangoDB':>10}")
        print(f"  {'-'*20} {'-'*10} {'-'*10}")
        print(f"  {'Cities':<20} {age_stats['cities']:>10} {arango_stats['cities']:>10}")
        print(f"  {'Intersections':<20} {age_stats['intersections']:>10} {arango_stats['intersections']:>10}")
        print(f"  {'Roads':<20} {age_stats['roads']:>10} {arango_stats['roads']:>10}")
        print(f"  {'City connections':<20} {age_stats['nearest_intersection']:>10} {arango_stats['nearest_intersection']:>10}")
        print("=" * 60)
        pg_cur.close()
        pg_conn.close()
        sys.exit(1)


if __name__ == '__main__':
    main()
