# AGE vs ArangoDB Benchmark

Benchmarking **Apache AGE** (PostgreSQL extension) vs **ArangoDB** using Alabama road network data from OpenStreetMap and population data from the US Census.

## Overview

This project loads identical data into both graph databases, enabling direct comparison of query syntax (Cypher vs AQL) and performance characteristics.

**Requires:** Docker<br>
**Developed on:** Windows with WSL2 (Debian)

## Architecture

```
                    +------------------+
                    |   Alabama OSM    |
                    | (Overpass API)   |
                    +--------+---------+
                             |
              +--------------+--------------+
              |                             |
              v                             v
    +------------------+          +------------------+
    | PostgreSQL + AGE |          |    ArangoDB      |
    | localhost:5432   |          | localhost:8529   |
    +------------------+          +------------------+
              |                             |
       +------+------+               +------+------+
       |             |               |             |
       v             v               v             v
    +------+     +-------+        +------+     +-------+
    |Cities|     | Roads |        |Cities|     | Roads |
    +------+     +-------+        +------+     +-------+
```

The project loads identical data into both databases for direct query and performance comparison.

### Graph Schema

**Nodes:**
- `:City` — Alabama's 5 most populous cities (2020 census) with `population`, `tourist_attractions_count`
- `:Intersection` — highway junction points

**Edges:**
- `[:ROAD]` — `length_miles`, `speed_mph`, `travel_time_s`
- `[:NEAREST_INTERSECTION]` — city-to-highway connections

---

## Quick start

Run these commands to get started:

```bash
make up       # Start the Docker containers
make setup    # Download data, build graphs, verify data consistency across both databases
make benchmark # Compare query performance
```

**Containers:** PostgreSQL + AGE (5432), ArangoDB (8529), pgAdmin (5050)

---

## Cleanup and shutdown

Stop containers with `make down`, remove downloaded data with `make clean`, or do a full reset including volumes with `make reset`.

---

## Benchmark results

Sample output from my laptop (WSL2/Debian). Results vary by hardware:

```
  Query                               | PostgreSQL + AGE | ArangoDB     | Winner
  -------------------------------------------------------------------------------
  1. Tourist Attractions Correlation   |          0.55 ms |      1.82 ms | PostgreSQL + AGE (3.3x)
  2. Quickest Route HSV→BHM           |          3.50 ms |    365.08 ms | PostgreSQL + AGE (104.2x)
  3. Slowest Route HSV→MOB            |          2.10 ms |      3.24 ms | PostgreSQL + AGE (1.5x)
  4. BHM→MGM Route Options            |          5.29 ms |      8.59 ms | PostgreSQL + AGE (1.6x)
  5. Cities Within a 2 Hour Drive     |          1.73 ms |      6.08 ms | PostgreSQL + AGE (3.5x)

  AVERAGE: PostgreSQL + AGE 2.63 ms | ArangoDB 76.96 ms
  OVERALL WINNER: PostgreSQL + AGE (29.2x faster)
```

---

## Database access

### pgAdmin (PostgreSQL + AGE)

URL: http://localhost:5050

| Field | Value |
|-------|-------|
| Email | `admin@example.com` |
| Password | `admin` |

Server connection: Host `age`, Port `5432`, Database `alabama_osm`, User `osm`, Password `osm_password`

### ArangoDB Web UI

URL: http://localhost:8529

| Field | Value |
|-------|-------|
| Username | `root` |
| Password | `osm_password` |
| Database | `alabama_osm` |

---

## AGE Indexing for Graph Traversal

Apache AGE stores graph data in PostgreSQL tables-nodes and edges each get their own table within the graph schema. Without indexes, every graph traversal requires full table scans to find connected edges, making queries extremely slow as the graph grows.

**Key indexes for traversal performance:**

| Index | Purpose |
|-------|---------|
| `BTREE (start_id)` on edges | Fast lookup of outgoing edges from a node |
| `BTREE (end_id)` on edges | Fast lookup of incoming edges to a node |
| `BTREE (start_id, end_id)` on edges | Optimizes bidirectional path queries |
| `BTREE (id)` on nodes | Fast node lookups by internal ID |
| `GIN (properties)` on nodes/edges | Fast property-based filtering (e.g., `{name: 'I-65'}`) |

**Why this matters:** A Cypher query like `MATCH (a)-[:ROAD*3]->(b)` must find all ROAD edges starting from node `a`, then repeat for each intermediate node. Without a `start_id` index, each hop scans the entire edge table. With indexes, each hop is an O(log n) lookup instead of O(n).

The `make setup` command creates these indexes automatically. For manual creation:

```sql
CREATE INDEX idx_road_start ON alabama_routing."ROAD" USING BTREE (start_id);
CREATE INDEX idx_road_end ON alabama_routing."ROAD" USING BTREE (end_id);
CREATE INDEX idx_road_props ON alabama_routing."ROAD" USING GIN (properties);
```

---

## AGE Queries (Cypher)

Initialize AGE at the start of each pgAdmin session:

```sql
LOAD 'age';
SET search_path = ag_catalog, "$user", public;
```

### Tourist Attractions Correlation

```sql
WITH city_stats AS (
    SELECT
        city::text AS city,
        pop::text::float AS population,
        ta::text::float AS tourist_attractions
    FROM cypher('alabama_routing', $$
        MATCH (c:City)
        RETURN c.name AS city, c.population AS pop, c.tourist_attractions_count AS ta
    $$) AS (city agtype, pop agtype, ta agtype)
),
max_vals AS (
    SELECT max(population) AS max_pop, max(tourist_attractions) AS max_ta
    FROM city_stats
)
SELECT
    cs.city,
    cs.population::int AS population,
    cs.tourist_attractions::int AS tourist_attractions,
    round((cs.tourist_attractions / cs.population * 10000)::numeric, 2) AS tourist_attractions_per_10k_residents,
    round(((cs.tourist_attractions / m.max_ta) / (cs.population / m.max_pop))::numeric, 2) AS correlation
FROM city_stats cs, max_vals m
ORDER BY cs.population DESC;
```

### Quickest Route - Huntsville to Birmingham

```sql
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
  $$) AS (route agtype, distance_miles agtype, drive_time_minutes agtype, road_segments agtype);
```

### Slowest Route - Huntsville to Mobile

```sql
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
$$) AS (route agtype, distance_miles agtype, drive_time_minutes agtype, road_segments agtype);
```

### Route Options - Birmingham to Montgomery

```sql
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
$$) AS (route_via agtype, distance_miles agtype, drive_time_minutes agtype);
```

### Cities Within a 2 Hour Drive 

```sql
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
ORDER BY cities_within_120_min DESC;
```

---

## ArangoDB Queries (AQL)

Run in the ArangoDB Web UI Queries section.

### Tourist Attractions Correlation

```aql
LET max_pop = MAX(FOR c IN cities RETURN c.population)
LET max_ta = MAX(FOR c IN cities RETURN c.tourist_attractions_count)

FOR c IN cities
    LET ta_per_10k = (c.tourist_attractions_count / c.population) * 10000
    LET correlation = (c.tourist_attractions_count / max_ta) / (c.population / max_pop)
    SORT c.population DESC
    RETURN {
        city: c.name,
        population: c.population,
        tourist_attractions: c.tourist_attractions_count,
        tourist_attractions_per_10k_residents: ROUND(ta_per_10k * 100) / 100,
        correlation: ROUND(correlation * 100) / 100
    }
```

### Quickest Route - Huntsville to Birmingham

```aql
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
```

### Slowest Route - Huntsville to Mobile

```aql
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
```

### Route Options - Birmingham to Montgomery

```aql
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
```

### Cities Within a 2 Hour Drive

```aql
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
```

