#!/bin/bash
# Start database containers for benchmarking

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Starting database containers..."

# Start specific database or all
DB=${1:-all}

case $DB in
  postgres|pg)
    echo "Starting PostgreSQL..."
    docker-compose up -d postgres
    echo "Waiting for PostgreSQL to be ready..."
    docker-compose exec -T postgres pg_isready -U user -d test_db
    echo "PostgreSQL is ready at localhost:5432"
    echo "  Database: test_db"
    echo "  User: user"
    echo "  Password: password"
    ;;
  mysql)
    echo "Starting MySQL..."
    docker-compose up -d mysql
    echo "Waiting for MySQL to be ready..."
    until docker-compose exec -T mysql mysqladmin ping -h localhost -u user -ppassword --silent; do
      echo "  Waiting for MySQL..."
      sleep 2
    done
    echo "MySQL is ready at localhost:3306"
    echo "  Database: test_db"
    echo "  User: user"
    echo "  Password: password"
    ;;
  oracle)
    echo "Starting Oracle XE..."
    docker-compose up -d oracle
    echo "Waiting for Oracle XE to be ready (this may take 2-3 minutes)..."
    until docker-compose exec -T oracle bash -c "echo 'SELECT 1 FROM DUAL;' | sqlplus -S system/Password123!@localhost/XEPDB1" 2>/dev/null | grep -q "^         1"; do
      echo "  Waiting for Oracle..."
      sleep 10
    done
    echo "Oracle XE is ready at localhost:1521"
    echo "  Service: XEPDB1"
    echo "  User: system"
    echo "  Password: Password123!"
    ;;
  mssql)
    echo "Starting MS SQL Server..."
    docker-compose up -d mssql
    echo "Waiting for MS SQL Server to be ready..."
    until docker-compose exec -T mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'Password123!' -C -Q 'SELECT 1' >/dev/null 2>&1; do
      echo "  Waiting for MS SQL Server..."
      sleep 3
    done
    echo "MS SQL Server is ready at localhost:1433"
    echo "  User: sa"
    echo "  Password: Password123!"
    ;;
  db2)
    echo "Starting IBM DB2..."
    docker-compose up -d db2
    echo "Waiting for DB2 to be ready (this may take 5+ minutes)..."
    until docker-compose exec -T db2 su - db2inst1 -c "db2 connect to test_db" >/dev/null 2>&1; do
      echo "  Waiting for DB2..."
      sleep 10
    done
    echo "DB2 is ready at localhost:50000"
    echo "  Database: test_db"
    echo "  User: db2inst1"
    echo "  Password: Password123!"
    ;;
  drill)
    echo "Starting Apache Drill..."
    docker-compose up -d drill
    echo "Waiting for Drill to be ready..."
    until curl -sf http://localhost:8047/stats.json >/dev/null 2>&1; do
      echo "  Waiting for Drill..."
      sleep 5
    done
    echo "Drill is ready at localhost:31010 (JDBC), localhost:8047 (Web UI)"
    ;;
  trino)
    echo "Starting Trino..."
    docker-compose up -d trino
    echo "Waiting for Trino to be ready..."
    until curl -sf http://localhost:8080/v1/info >/dev/null 2>&1; do
      echo "  Waiting for Trino..."
      sleep 3
    done
    echo "Trino is ready at localhost:8080"
    echo "  Catalog: memory"
    echo "  Schema: default"
    ;;
  all)
    echo "Starting all databases..."
    docker-compose up -d

    echo "Waiting for PostgreSQL..."
    until docker-compose exec -T postgres pg_isready -U user -d test_db 2>/dev/null; do
      echo "  Waiting for PostgreSQL..."
      sleep 2
    done
    echo "PostgreSQL is ready at localhost:5432"

    echo "Waiting for MySQL..."
    until docker-compose exec -T mysql mysqladmin ping -h localhost -u user -ppassword --silent 2>/dev/null; do
      echo "  Waiting for MySQL..."
      sleep 2
    done
    echo "MySQL is ready at localhost:3306"

    echo "Waiting for Oracle XE..."
    until docker-compose exec -T oracle bash -c "echo 'SELECT 1 FROM DUAL;' | sqlplus -S system/Password123!@localhost/XEPDB1" 2>/dev/null | grep -q "^         1"; do
      echo "  Waiting for Oracle..."
      sleep 10
    done
    echo "Oracle XE is ready at localhost:1521"

    echo "Waiting for MS SQL Server..."
    until docker-compose exec -T mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'Password123!' -C -Q 'SELECT 1' >/dev/null 2>&1; do
      echo "  Waiting for MS SQL Server..."
      sleep 3
    done
    echo "MS SQL Server is ready at localhost:1433"

    echo "Waiting for DB2..."
    until docker-compose exec -T db2 su - db2inst1 -c "db2 connect to test_db" >/dev/null 2>&1; do
      echo "  Waiting for DB2..."
      sleep 10
    done
    echo "DB2 is ready at localhost:50000"

    echo "Waiting for Drill..."
    until curl -sf http://localhost:8047/stats.json >/dev/null 2>&1; do
      echo "  Waiting for Drill..."
      sleep 5
    done
    echo "Drill is ready at localhost:31010"

    echo "Waiting for Trino..."
    until curl -sf http://localhost:8080/v1/info >/dev/null 2>&1; do
      echo "  Waiting for Trino..."
      sleep 3
    done
    echo "Trino is ready at localhost:8080"

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
