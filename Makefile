.PHONY: up down data setup clean reset benchmark

# Start Docker containers
up:
	@echo "Starting Docker containers..."
	docker compose up -d --build
	@echo ""
	@echo "Waiting for containers to start..."
	@until docker compose ps 2>/dev/null | grep -q "osm_age_db.*Up"; do \
		sleep 1; \
	done
	@echo "Waiting for PostgreSQL to be healthy..."
	@until docker compose exec -T age pg_isready -U osm -d alabama_osm > /dev/null 2>&1; do \
		echo "  Waiting for PostgreSQL..."; \
		sleep 2; \
	done
	@echo "PostgreSQL is ready!"
	@echo ""
	@echo "Waiting for ArangoDB to be ready..."
	@until docker compose exec -T arangodb arangosh --server.password osm_password --javascript.execute-string "db._version()" > /dev/null 2>&1; do \
		echo "  Waiting for ArangoDB..."; \
		sleep 3; \
	done
	@echo "ArangoDB is ready!"
	@echo ""
	docker compose ps

# Stop Docker containers
down:
	docker compose down --remove-orphans

# Full reset - stop containers, remove volumes, and clean data files
reset: clean
	docker compose down --remove-orphans -v
	@echo "Reset complete - all data removed"

# Download OSM data, import into PostgreSQL, and build both graphs
data:
	@echo "============================================================"
	@echo "Building graphs..."
	@echo "============================================================"
	@echo ""
	@echo "[Step 1/4] Downloading Alabama OSM data from Overpass API..."
	@docker compose exec age python3 /scripts/osm_download.py || { echo "Download failed"; exit 1; }
	@echo ""
	@echo "[Step 2/4] Importing OSM and census data into PostgreSQL..."
	@docker compose exec age python3 /scripts/import_osm_and_census_data.py /data/alabama/alabama.osm || { echo "Import failed"; exit 1; }
	@echo ""
	@echo "[Step 3/4] Building PostgreSQL + AGE graph..."
	@docker compose exec age python3 /scripts/build_graph.py --host age || { echo "AGE graph build failed"; exit 1; }
	@echo ""
	@echo "[Step 4/4] Building ArangoDB graph..."
	@docker compose exec age python3 /scripts/build_graph_arango.py --arango-host arangodb --pg-host age || { echo "ArangoDB graph build failed"; exit 1; }
	@echo ""
	@echo "============================================================"
	@echo "DATA IMPORT COMPLETE"
	@echo "============================================================"
	@echo ""
	@echo "Both databases now contain the same graph data:"
	@echo "  - Cities (from PostgreSQL census data)"
	@echo "  - Intersections (hardcoded demo nodes)"
	@echo "  - Road segments (hardcoded demo edges)"
	@echo "  - City-to-intersection connections"
	@echo ""
	@echo "Next step: make benchmark"
	@echo ""
	@echo "Access points:"
	@echo "  pgAdmin:   http://localhost:5050"
	@echo "  ArangoDB:  http://localhost:8529"
	@echo "============================================================"

# Full setup: download data, build graphs, and verify
setup: data
	@echo "Verifying both databases have identical data..."
	@docker compose exec age python3 /scripts/verify_graphs.py --pg-host age --arango-host arangodb
	@echo ""
	@echo "============================================================"
	@echo "SETUP COMPLETE!"
	@echo "============================================================"
	@echo ""
	@echo "Next step: make benchmark"
	@echo ""

# Run query performance benchmarks with result verification
benchmark:
	@echo "Running query performance benchmarks..."
	@echo "This will:"
	@echo "  1. Execute identical queries on both databases"
	@echo "  2. Verify that results are IDENTICAL"
	@echo "  3. Compare execution times"
	@echo ""
	@docker compose exec age python3 /scripts/benchmark_queries.py --pg-host age --arango-host arangodb --runs 5

# Remove downloaded data files
clean:
	rm -rf data/alabama/*.osm data/alabama/*.osm.pbf
	@echo "Cleaned data files"
