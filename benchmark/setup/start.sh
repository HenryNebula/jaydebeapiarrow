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
    echo "✓ PostgreSQL is ready at localhost:5432"
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
    echo "✓ MySQL is ready at localhost:3306"
    echo "  Database: test_db"
    echo "  User: user"
    echo "  Password: password"
    ;;
  all)
    echo "Starting all databases..."
    docker-compose up -d

    echo "Waiting for PostgreSQL..."
    until docker-compose exec -T postgres pg_isready -U user -d test_db 2>/dev/null; do
      echo "  Waiting for PostgreSQL..."
      sleep 2
    done
    echo "✓ PostgreSQL is ready at localhost:5432"

    echo "Waiting for MySQL..."
    until docker-compose exec -T mysql mysqladmin ping -h localhost -u user -ppassword --silent 2>/dev/null; do
      echo "  Waiting for MySQL..."
      sleep 2
    done
    echo "✓ MySQL is ready at localhost:3306"

    echo ""
    echo "All databases are ready!"
    echo ""
    echo "PostgreSQL:"
    echo "  Host: localhost:5432"
    echo "  Database: test_db"
    echo "  User: user"
    echo "  Password: password"
    echo ""
    echo "MySQL:"
    echo "  Host: localhost:3306"
    echo "  Database: test_db"
    echo "  User: user"
    echo "  Password: password"
    ;;
  *)
    echo "Usage: $0 [postgres|mysql|all]"
    echo "  postgres - Start only PostgreSQL"
    echo "  mysql     - Start only MySQL"
    echo "  all       - Start both databases (default)"
    exit 1
    ;;
esac
