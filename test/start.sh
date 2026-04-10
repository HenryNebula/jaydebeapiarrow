#!/bin/bash
# Start database containers for testing

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Load .env if it exists
if [ -f .env ]; then
    set -a; source .env; set +a
fi

# Apply defaults for any values not in .env
: "${POSTGRES_PORT:=15432}" "${POSTGRES_DB:=test_db}" "${POSTGRES_USER:=user}" "${POSTGRES_PASSWORD:=password}"
: "${MYSQL_PORT:=13306}" "${MYSQL_DATABASE:=test_db}" "${MYSQL_USER:=user}" "${MYSQL_PASSWORD:=password}"
: "${ORACLE_PORT:=11521}" "${ORACLE_PASSWORD:=Password123!}"
: "${MSSQL_PORT:=11433}" "${MSSQL_SA_PASSWORD:=Password123!}"
: "${DB2_PORT:=15000}" "${DB2_PASSWORD:=Password123!}" "${DB2_DBNAME:=test_db}"
: "${DRILL_PORT:=31010}" "${DRILL_WEB_PORT:=18047}"
: "${TRINO_PORT:=18080}"

echo "Starting database containers..."

DB=${1:-all}

wait_for_healthy() {
    local container="$1"
    local max_wait="${2:-300}"
    local elapsed=0
    while [ $elapsed -lt $max_wait ]; do
        status=$(docker inspect --format '{{.State.Health.Status}}' "$container" 2>/dev/null || echo "missing")
        if [ "$status" = "healthy" ]; then
            return 0
        fi
        sleep 5
        elapsed=$((elapsed + 5))
    done
    echo "  WARNING: $container not healthy after ${max_wait}s (status: $status)"
    return 1
}

case $DB in
  postgres|pg)
    echo "Starting PostgreSQL..."
    docker compose up -d postgres
    wait_for_healthy jaydebeapi-benchmark-postgres 30
    echo "PostgreSQL is ready at localhost:${POSTGRES_PORT}"
    echo "  Database: ${POSTGRES_DB}"
    echo "  User: ${POSTGRES_USER}"
    echo "  Password: ${POSTGRES_PASSWORD}"
    ;;
  mysql)
    echo "Starting MySQL..."
    docker compose up -d mysql
    wait_for_healthy jaydebeapi-benchmark-mysql 30
    echo "MySQL is ready at localhost:${MYSQL_PORT}"
    echo "  Database: ${MYSQL_DATABASE}"
    echo "  User: ${MYSQL_USER}"
    echo "  Password: ${MYSQL_PASSWORD}"
    ;;
  oracle)
    echo "Starting Oracle XE..."
    docker compose up -d oracle
    echo "Waiting for Oracle XE to be ready (this may take 2-3 minutes)..."
    wait_for_healthy jaydebeapi-test-oracle 300
    echo "Oracle XE is ready at localhost:${ORACLE_PORT}"
    echo "  Service: XEPDB1"
    echo "  User: system"
    echo "  Password: ${ORACLE_PASSWORD}"
    ;;
  mssql)
    echo "Starting MS SQL Server..."
    docker compose up -d mssql
    wait_for_healthy jaydebeapi-test-mssql 60
    echo "MS SQL Server is ready at localhost:${MSSQL_PORT}"
    echo "  User: sa"
    echo "  Password: ${MSSQL_SA_PASSWORD}"
    ;;
  db2)
    echo "Starting IBM DB2..."
    docker compose up -d db2
    echo "Waiting for DB2 to be ready (this may take 5+ minutes)..."
    wait_for_healthy jaydebeapi-test-db2 600
    echo "DB2 is ready at localhost:${DB2_PORT}"
    echo "  Database: ${DB2_DBNAME}"
    echo "  User: db2inst1"
    echo "  Password: ${DB2_PASSWORD}"
    ;;
  drill)
    echo "Starting Apache Drill..."
    docker compose up -d drill
    wait_for_healthy jaydebeapi-test-drill 120
    echo "Drill is ready at localhost:${DRILL_PORT} (JDBC), localhost:${DRILL_WEB_PORT} (Web UI)"
    ;;
  trino)
    echo "Starting Trino..."
    docker compose up -d trino
    wait_for_healthy jaydebeapi-test-trino 60
    echo "Creating memory catalog..."
    docker compose exec -T trino trino --execute 'CREATE CATALOG memory USING memory;' 2>/dev/null || echo "  Catalog may already exist"
    echo "Trino is ready at localhost:${TRINO_PORT}"
    echo "  Catalog: memory"
    echo "  Schema: default"
    ;;
  all)
    echo "Starting all databases..."
    docker compose up -d

    echo "Waiting for PostgreSQL..."
    wait_for_healthy jaydebeapi-benchmark-postgres 30
    echo "  PostgreSQL ready at localhost:${POSTGRES_PORT}"

    echo "Waiting for MySQL..."
    wait_for_healthy jaydebeapi-benchmark-mysql 30
    echo "  MySQL ready at localhost:${MYSQL_PORT}"

    echo "Waiting for Oracle XE..."
    wait_for_healthy jaydebeapi-test-oracle 300
    echo "  Oracle ready at localhost:${ORACLE_PORT}"

    echo "Waiting for MS SQL Server..."
    wait_for_healthy jaydebeapi-test-mssql 60
    echo "  MSSQL ready at localhost:${MSSQL_PORT}"

    echo "Waiting for DB2..."
    wait_for_healthy jaydebeapi-test-db2 600
    echo "  DB2 ready at localhost:${DB2_PORT}"

    echo "Waiting for Drill..."
    wait_for_healthy jaydebeapi-test-drill 120
    echo "  Drill ready at localhost:${DRILL_PORT}"

    echo "Waiting for Trino..."
    wait_for_healthy jaydebeapi-test-trino 60
    echo "Creating Trino memory catalog..."
    docker compose exec -T trino trino --execute 'CREATE CATALOG memory USING memory;' 2>/dev/null || echo "  Catalog may already exist"
    echo "  Trino ready at localhost:${TRINO_PORT}"

    echo ""
    echo "All databases are ready!"
    ;;
  *)
    echo "Usage: $0 [postgres|mysql|oracle|mssql|db2|drill|trino|all]"
    echo "  postgres - Start only PostgreSQL"
    echo "  mysql    - Start only MySQL"
    echo "  oracle   - Start only Oracle XE"
    echo "  mssql    - Start only MS SQL Server"
    echo "  db2      - Start only IBM DB2"
    echo "  drill    - Start only Apache Drill"
    echo "  trino    - Start only Trino"
    echo "  all      - Start all databases (default)"
    exit 1
    ;;
esac
