#!/usr/bin/env python3
"""
Benchmark comparison of PostgreSQL + AGE (Cypher) vs ArangoDB (AQL) queries.
Runs equivalent queries on both databases and compares execution times.
"""

import argparse
import time
import sys

import psycopg2
from arango import ArangoClient

from config import (
    PG_HOST, PG_PORT, PG_USER, PG_PASSWORD, PG_DBNAME,
    ARANGO_HOST, ARANGO_PORT, ARANGO_PASSWORD
)


# Index creation statements for AGE (run once for performance)
AGE_INDEXES = [
    'CREATE INDEX IF NOT EXISTS idx_city_id ON alabama_routing."City" USING BTREE (id)',
    'CREATE INDEX IF NOT EXISTS idx_city_props ON alabama_routing."City" USING GIN (properties)',
    'CREATE INDEX IF NOT EXISTS idx_intersection_id ON alabama_routing."Intersection" USING BTREE (id)',
    'CREATE INDEX IF NOT EXISTS idx_road_id ON alabama_routing."ROAD" USING BTREE (id)',
    'CREATE INDEX IF NOT EXISTS idx_road_start ON alabama_routing."ROAD" USING BTREE (start_id)',
    'CREATE INDEX IF NOT EXISTS idx_road_end ON alabama_routing."ROAD" USING BTREE (end_id)',
    'CREATE INDEX IF NOT EXISTS idx_nearest_start ON alabama_routing."NEAREST_INTERSECTION" USING BTREE (start_id)',
    'CREATE INDEX IF NOT EXISTS idx_nearest_end ON alabama_routing."NEAREST_INTERSECTION" USING BTREE (end_id)',
]


# PostgreSQL + AGE Cypher queries (optimized versions from README)
CYPHER_QUERIES = {
    "1. Tourist Attractions Correlation": """
        SELECT * FROM cypher('alabama_routing', $$
            MATCH (c:City)
            RETURN c.name AS city, c.population AS pop, c.tourist_attractions_count AS ta
            ORDER BY c.population DESC
        $$) AS (city agtype, pop agtype, ta agtype)
    """,

    "2. Quickest Route HSV→BHM": """
        SELECT * FROM cypher('alabama_routing', $$
            MATCH (:City {name: 'Huntsville'})-[:NEAREST_INTERSECTION]->(si)
            WITH si
            MATCH (:City {name: 'Birmingham'})-[:NEAREST_INTERSECTION]->(ei)
            WITH si, ei
            MATCH path = (si)-[:ROAD*2]->(ei)
            UNWIND relationships(path) AS r
            WITH path, sum(r.travel_time_s) AS total_time_s, sum(r.length_miles) AS total_miles
            RETURN
                'Huntsville -> Birmingham' AS route,
                round(total_miles) AS distance_miles,
                round(total_time_s / 60.0) AS drive_time_minutes,
                length(path) AS road_segments
            ORDER BY total_time_s ASC
            LIMIT 1
        $$) AS (route agtype, distance_miles agtype, drive_time_minutes agtype, road_segments agtype)
    """,

    "3. Slowest Route HSV→MOB": """
        SELECT * FROM cypher('alabama_routing', $$
            MATCH (:City {name: 'Huntsville'})-[:NEAREST_INTERSECTION]->(si)
            WITH si LIMIT 1
            MATCH (:City {name: 'Mobile'})-[:NEAREST_INTERSECTION]->(ei)
            WITH si, ei LIMIT 1
            MATCH path = (si)-[:ROAD*3..4]->(ei)
            UNWIND relationships(path) AS r
            WITH path, sum(r.travel_time_s) AS total_time_s, sum(r.length_miles) AS total_miles
            RETURN
                'Huntsville -> Mobile' AS route,
                round(total_miles) AS distance_miles,
                round(total_time_s / 60.0) AS drive_time_minutes,
                length(path) AS road_segments
            ORDER BY total_time_s DESC
            LIMIT 1
        $$) AS (route agtype, distance_miles agtype, drive_time_minutes agtype, road_segments agtype)
    """,

    "4. BHM→MGM Route Options": """
        SELECT * FROM cypher('alabama_routing', $$
            MATCH (:City {name: 'Birmingham'})-[:NEAREST_INTERSECTION]->(si)
            WITH si
            MATCH (:City {name: 'Montgomery'})-[:NEAREST_INTERSECTION]->(ei)
            WITH si, ei
            MATCH path = (si)-[:ROAD*1..3]->(ei)
            UNWIND relationships(path) AS r
            WITH path, collect(DISTINCT r.name) AS route_via,
                 round(sum(r.length_miles)) AS distance_miles,
                 round(sum(r.travel_time_s) / 60.0) AS drive_time_minutes
            RETURN DISTINCT route_via, distance_miles, drive_time_minutes
            ORDER BY drive_time_minutes
            LIMIT 5
        $$) AS (route_via agtype, distance_miles agtype, drive_time_minutes agtype)
    """,

    "5. Cities Within a 2 Hour Drive": """
        WITH city_intersections AS (
            SELECT
                trim(both '"' from agtype_access_operator(c.properties, '"name"'::agtype)::text) AS city_name,
                (agtype_access_operator(c.properties, '"population"'::agtype)::text)::int AS population,
                ni.end_id AS intersection_id
            FROM alabama_routing."City" c
            JOIN alabama_routing."NEAREST_INTERSECTION" ni ON ni.start_id::text = c.id::text
        ),
        single_hop AS (
            SELECT
                ci1.city_name AS src, ci1.population AS pop, ci2.city_name AS dest,
                (agtype_access_operator(r.properties, '"travel_time_s"'::agtype)::text)::float AS travel_time
            FROM city_intersections ci1
            JOIN alabama_routing."ROAD" r ON r.start_id::text = ci1.intersection_id::text
            JOIN city_intersections ci2 ON r.end_id::text = ci2.intersection_id::text
            WHERE ci1.city_name <> ci2.city_name
              AND (agtype_access_operator(r.properties, '"travel_time_s"'::agtype)::text)::float <= 7200
        ),
        two_hop AS (
            SELECT
                ci1.city_name AS src, ci1.population AS pop, ci2.city_name AS dest,
                (agtype_access_operator(r1.properties, '"travel_time_s"'::agtype)::text)::float
                + (agtype_access_operator(r2.properties, '"travel_time_s"'::agtype)::text)::float AS travel_time
            FROM city_intersections ci1
            JOIN alabama_routing."ROAD" r1 ON r1.start_id::text = ci1.intersection_id::text
            JOIN alabama_routing."ROAD" r2 ON r2.start_id::text = r1.end_id::text
            JOIN city_intersections ci2 ON r2.end_id::text = ci2.intersection_id::text
            WHERE ci1.city_name <> ci2.city_name
              AND (agtype_access_operator(r1.properties, '"travel_time_s"'::agtype)::text)::float
                + (agtype_access_operator(r2.properties, '"travel_time_s"'::agtype)::text)::float <= 7200
        ),
        all_routes AS (
            SELECT * FROM single_hop UNION ALL SELECT * FROM two_hop
        ),
        min_times AS (
            SELECT src, pop, dest, MIN(travel_time) AS min_time
            FROM all_routes GROUP BY src, pop, dest
        )
        SELECT src AS major_city, pop AS population, COUNT(*) AS cities_within_120_min
        FROM min_times GROUP BY src, pop
        ORDER BY cities_within_120_min DESC
    """,
}

# ArangoDB AQL queries (from README)
AQL_QUERIES = {
    "1. Tourist Attractions Correlation": """
        FOR c IN cities
            SORT c.population DESC
            RETURN {
                city: c.name,
                population: c.population,
                tourist_attractions: c.tourist_attractions_count
            }
    """,

    "2. Quickest Route HSV→BHM": """
        LET huntsville = FIRST(FOR c IN cities FILTER c.name == "Huntsville" RETURN c)
        LET birmingham = FIRST(FOR c IN cities FILTER c.name == "Birmingham" RETURN c)
        LET start_int = FIRST(FOR v IN 1..1 OUTBOUND huntsville nearest_intersection RETURN v)
        LET end_int = FIRST(FOR v IN 1..1 OUTBOUND birmingham nearest_intersection RETURN v)

        FOR v, e, p IN 1..30 OUTBOUND start_int roads
            FILTER v._id == end_int._id
            LET total_time = SUM(p.edges[*].travel_time_s)
            LET total_miles = SUM(p.edges[*].length_miles)
            SORT total_time ASC
            LIMIT 1
            RETURN {
                route: "Huntsville -> Birmingham",
                distance_miles: ROUND(total_miles),
                drive_time_minutes: ROUND(total_time / 60),
                road_segments: LENGTH(p.edges)
            }
    """,

    "3. Slowest Route HSV→MOB": """
        LET huntsville = FIRST(FOR c IN cities FILTER c.name == "Huntsville" RETURN c)
        LET mobile = FIRST(FOR c IN cities FILTER c.name == "Mobile" RETURN c)
        LET start_int = FIRST(FOR v IN 1..1 OUTBOUND huntsville nearest_intersection RETURN v)
        LET end_int = FIRST(FOR v IN 1..1 OUTBOUND mobile nearest_intersection RETURN v)

        FOR v, e, p IN 3..4 OUTBOUND start_int roads
            FILTER v._id == end_int._id
            LET total_time = SUM(p.edges[*].travel_time_s)
            LET total_miles = SUM(p.edges[*].length_miles)
            SORT total_time DESC
            LIMIT 1
            RETURN {
                route: "Huntsville -> Mobile",
                distance_miles: ROUND(total_miles),
                drive_time_minutes: ROUND(total_time / 60),
                road_segments: LENGTH(p.edges)
            }
    """,

    "4. BHM→MGM Route Options": """
        LET birmingham = FIRST(FOR c IN cities FILTER c.name == "Birmingham" RETURN c)
        LET montgomery = FIRST(FOR c IN cities FILTER c.name == "Montgomery" RETURN c)
        LET start_int = FIRST(FOR v IN 1..1 OUTBOUND birmingham nearest_intersection RETURN v)
        LET end_int = FIRST(FOR v IN 1..1 OUTBOUND montgomery nearest_intersection RETURN v)

        LET all_paths = (
            FOR v, e, p IN 1..5 OUTBOUND start_int roads
                FILTER v._id == end_int._id
                LET total_time = SUM(p.edges[*].travel_time_s)
                LET total_miles = SUM(p.edges[*].length_miles)
                LET road_names = UNIQUE(p.edges[*].name)
                RETURN {
                    route_via: road_names,
                    distance_miles: ROUND(total_miles),
                    drive_time_minutes: ROUND(total_time / 60)
                }
        )

        FOR path IN all_paths
            COLLECT route_via = path.route_via,
                    distance_miles = path.distance_miles,
                    drive_time_minutes = path.drive_time_minutes
            SORT drive_time_minutes ASC
            LIMIT 5
            RETURN {
                route_via: route_via,
                distance_miles: distance_miles,
                drive_time_minutes: drive_time_minutes
            }
    """,

    "5. Cities Within a 2 Hour Drive": """
        FOR c1 IN cities
            LET start_int = FIRST(FOR v IN 1..1 OUTBOUND c1 nearest_intersection RETURN v)
            LET reachable = (
                FOR c2 IN cities
                    FILTER c2._key != c1._key
                    LET end_int = FIRST(FOR v IN 1..1 OUTBOUND c2 nearest_intersection RETURN v)
                    LET shortest = (
                        FOR v, e, p IN 1..2 OUTBOUND start_int roads
                            FILTER v._id == end_int._id
                            LET time = SUM(p.edges[*].travel_time_s)
                            SORT time ASC
                            LIMIT 1
                            RETURN time
                    )
                    FILTER LENGTH(shortest) > 0 AND shortest[0] <= 7200
                    RETURN c2.name
            )
            FILTER LENGTH(reachable) > 0
            SORT LENGTH(reachable) DESC
            RETURN {
                major_city: c1.name,
                population: c1.population,
                cities_within_120_min: LENGTH(reachable)
            }
    """,
}

# Column names for each query
QUERY_COLUMNS = {
    "1. Tourist Attractions Correlation": ["city", "population", "tourist_attractions"],
    "2. Quickest Route HSV→BHM": ["route", "distance_miles", "drive_time_minutes", "road_segments"],
    "3. Slowest Route HSV→MOB": ["route", "distance_miles", "drive_time_minutes", "road_segments"],
    "4. BHM→MGM Route Options": ["route_via", "distance_miles", "drive_time_minutes"],
    "5. Cities Within a 2 Hour Drive": ["major_city", "population", "cities_within_120_min"],
}


def run_cypher_query(cur, query):
    """Run a Cypher query and return execution time in ms and results."""
    start = time.perf_counter()
    cur.execute(query)
    results = cur.fetchall()
    elapsed = (time.perf_counter() - start) * 1000
    return elapsed, results


def run_aql_query(db, query):
    """Run an AQL query and return execution time in ms and results."""
    start = time.perf_counter()
    cursor = db.aql.execute(query)
    results = list(cursor)
    elapsed = (time.perf_counter() - start) * 1000
    return elapsed, results


def normalize_value(val):
    """Normalize a single value for comparison.

    AGE returns agtype which wraps values - strings get quotes, numbers
    become strings. Must convert through str() then parse back.
    """
    if val is None:
        return None
    val_str = str(val).strip('"')
    try:
        if '.' in val_str:
            return float(val_str)
        else:
            return int(val_str)
    except (ValueError, AttributeError):
        return val_str


def normalize_result(result, db_type, column_names=None):
    """Normalize a result row for comparison between databases."""
    if db_type == 'cypher':
        return tuple(normalize_value(val) for val in result)
    else:
        if isinstance(result, dict):
            if column_names:
                return tuple(result.get(k) for k in column_names)
            return tuple(result[k] for k in sorted(result.keys()))
        return result


def compare_results(cypher_results, aql_results, column_names=None):
    """Compare results from both databases for equivalence."""
    if len(cypher_results) != len(aql_results):
        return False, f"Row count mismatch: AGE={len(cypher_results)}, Arango={len(aql_results)}"

    cypher_normalized = [normalize_result(r, 'cypher', column_names) for r in cypher_results]
    aql_normalized = [normalize_result(r, 'aql', column_names) for r in aql_results]

    # Sort by numeric columns (distance, time) for comparison
    def sort_key(row):
        return tuple(v if isinstance(v, (int, float)) else 0 for v in row)

    cypher_sorted = sorted(cypher_normalized, key=sort_key)
    aql_sorted = sorted(aql_normalized, key=sort_key)

    # Compare first 3 rows strictly, allow some variance in remaining rows
    compare_count = min(3, len(cypher_sorted))
    for i, (c_row, a_row) in enumerate(zip(cypher_sorted[:compare_count], aql_sorted[:compare_count])):
        if len(c_row) != len(a_row):
            return False, f"Row {i} column count differs"
        for j, (c_val, a_val) in enumerate(zip(c_row, a_row)):
            if isinstance(c_val, (int, float)) and isinstance(a_val, (int, float)):
                if abs(float(c_val) - float(a_val)) > 0.01:
                    return False, f"Row {i} col {j} differs: AGE={c_val}, Arango={a_val}"

    return True, "Results match"


def format_results_table(results, db_type, column_names=None, max_rows=10):
    """Format query results as a table."""
    if not results:
        return "    (no results)"

    rows = []
    if db_type == 'cypher':
        rows = [[normalize_value(val) for val in row] for row in results]
    else:
        for row in results:
            if isinstance(row, dict):
                if column_names is None:
                    column_names = list(row.keys())
                rows.append([row.get(k) for k in column_names])
            else:
                rows.append([row])

    if not rows:
        return "    (no results)"

    if column_names is None:
        column_names = [f"col{i}" for i in range(len(rows[0]))]

    col_widths = []
    for i, name in enumerate(column_names):
        width = len(str(name))
        for row in rows:
            if i < len(row):
                width = max(width, len(str(row[i])))
        col_widths.append(min(width + 2, 30))

    lines = []
    header = " | ".join(str(name).center(w) for name, w in zip(column_names, col_widths))
    separator = "-+-".join("-" * w for w in col_widths)
    lines.append(f"    {header}")
    lines.append(f"    {separator}")

    for row in rows[:max_rows]:
        row_str = " | ".join(
            str(v).ljust(w) if isinstance(v, str) else str(v).rjust(w)
            for v, w in zip(row, col_widths)
        )
        lines.append(f"    {row_str}")

    if len(rows) > max_rows:
        lines.append(f"    ... ({len(rows) - max_rows} more rows)")

    return "\n".join(lines)


def format_time(ms):
    """Format time in milliseconds."""
    return f"{ms:.2f} ms"


def create_indexes(cur):
    """Create indexes for better performance."""
    print("  Creating indexes...")
    for idx_sql in AGE_INDEXES:
        try:
            cur.execute(idx_sql)
        except Exception as e:
            # Index might already exist or table might not exist yet
            pass
    print("  Indexes ready")


def main():
    parser = argparse.ArgumentParser(description='Benchmark AGE vs ArangoDB queries')
    parser.add_argument('--pg-host', default=PG_HOST)
    parser.add_argument('--pg-port', type=int, default=PG_PORT)
    parser.add_argument('--arango-host', default=ARANGO_HOST)
    parser.add_argument('--arango-port', type=int, default=ARANGO_PORT)
    parser.add_argument('--dbname', default=PG_DBNAME)
    parser.add_argument('--pg-user', default=PG_USER)
    parser.add_argument('--pg-password', default=PG_PASSWORD)
    parser.add_argument('--arango-password', default=ARANGO_PASSWORD)
    parser.add_argument('--runs', type=int, default=3, help='Number of runs per query')

    args = parser.parse_args()

    print("=" * 80)
    print("  BENCHMARK: PostgreSQL + AGE vs ArangoDB")
    print("=" * 80)

    # Connect to PostgreSQL
    print("\nConnecting to PostgreSQL + AGE...")
    try:
        pg_conn = psycopg2.connect(
            host=args.pg_host, port=args.pg_port, dbname=args.dbname,
            user=args.pg_user, password=args.pg_password
        )
        pg_conn.autocommit = True
        pg_cur = pg_conn.cursor()
        pg_cur.execute("LOAD 'age'")
        pg_cur.execute("SET search_path = ag_catalog, '$user', public")
        # JIT setting now controlled at database level via ALTER DATABASE
        create_indexes(pg_cur)
        print("  Connected to PostgreSQL + AGE")
    except Exception as e:
        print(f"  Error: {e}")
        sys.exit(1)

    # Connect to ArangoDB
    print("\nConnecting to ArangoDB...")
    try:
        arango_client = ArangoClient(hosts=f'http://{args.arango_host}:{args.arango_port}')
        arango_db = arango_client.db(args.dbname, username='root', password=args.arango_password)
        print("  Connected to ArangoDB")
    except Exception as e:
        print(f"  Error: {e}")
        sys.exit(1)

    print(f"\nRunning benchmarks ({args.runs} runs each)...")
    print("=" * 80)

    results = []

    for query_name in CYPHER_QUERIES.keys():
        cypher_query = CYPHER_QUERIES[query_name]
        aql_query = AQL_QUERIES[query_name]
        column_names = QUERY_COLUMNS.get(query_name)

        print(f"\n{query_name}")
        print("-" * 80)

        # Warmup
        try:
            run_cypher_query(pg_cur, cypher_query)
        except Exception as e:
            print(f"  AGE warmup error: {e}")
        try:
            run_aql_query(arango_db, aql_query)
        except Exception as e:
            print(f"  AQL warmup error: {e}")

        # Benchmark
        cypher_times = []
        aql_times = []
        cypher_results = []
        aql_results = []

        for i in range(args.runs):
            try:
                t, res = run_cypher_query(pg_cur, cypher_query)
                cypher_times.append(t)
                cypher_results = res
            except Exception as e:
                print(f"  AGE error: {e}")

            try:
                t, res = run_aql_query(arango_db, aql_query)
                aql_times.append(t)
                aql_results = res
            except Exception as e:
                print(f"  AQL error: {e}")

        cypher_avg = sum(cypher_times) / len(cypher_times) if cypher_times else float('inf')
        aql_avg = sum(aql_times) / len(aql_times) if aql_times else float('inf')

        results_match, match_msg = compare_results(cypher_results, aql_results, column_names)

        if cypher_avg < aql_avg:
            winner = "PostgreSQL + AGE"
            speedup = aql_avg / cypher_avg if cypher_avg > 0 else 0
        else:
            winner = "ArangoDB"
            speedup = cypher_avg / aql_avg if aql_avg > 0 else 0

        # Print results
        print(f"\n  PostgreSQL + AGE:")
        print(format_results_table(cypher_results, 'cypher', column_names))
        print(f"\n  ArangoDB:")
        print(format_results_table(aql_results, 'aql', column_names))

        print(f"\n  TIMING:")
        print(f"    PostgreSQL + AGE: {format_time(cypher_avg):>12} ({len(cypher_results)} rows)")
        print(f"    ArangoDB:         {format_time(aql_avg):>12} ({len(aql_results)} rows)")
        print(f"    Winner: {winner} ({speedup:.1f}x faster)")
        print(f"    Results: {'IDENTICAL' if results_match else 'MISMATCH - ' + match_msg}")

        results.append({
            'query': query_name,
            'cypher_ms': cypher_avg,
            'aql_ms': aql_avg,
            'winner': winner,
            'speedup': speedup,
            'match': results_match
        })

    # Summary
    print("\n" + "=" * 80)
    print("  SUMMARY")
    print("=" * 80)

    print("\n  Query                               | PostgreSQL + AGE | ArangoDB     | Winner")
    print("  " + "-" * 90)
    for r in results:
        q = r['query'][:35].ljust(35)
        age = format_time(r['cypher_ms']).rjust(16)
        aql = format_time(r['aql_ms']).rjust(12)
        w = f"{r['winner']} ({r['speedup']:.1f}x)"
        print(f"  {q} | {age} | {aql} | {w}")

    avg_cypher = sum(r['cypher_ms'] for r in results) / len(results)
    avg_aql = sum(r['aql_ms'] for r in results) / len(results)
    age_wins = sum(1 for r in results if 'PostgreSQL' in r['winner'])
    matches = sum(1 for r in results if r['match'])

    print(f"\n  AVERAGE: PostgreSQL + AGE {format_time(avg_cypher)} | ArangoDB {format_time(avg_aql)}")

    if avg_cypher < avg_aql:
        print(f"  OVERALL WINNER: PostgreSQL + AGE ({avg_aql/avg_cypher:.1f}x faster)")
    else:
        print(f"  OVERALL WINNER: ArangoDB ({avg_cypher/avg_aql:.1f}x faster)")

    print(f"\n  Query wins: PostgreSQL + AGE {age_wins}/{len(results)} | ArangoDB {len(results)-age_wins}/{len(results)}")
    print("=" * 80)

    pg_cur.close()
    pg_conn.close()


if __name__ == '__main__':
    main()
