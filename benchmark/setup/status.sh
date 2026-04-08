#!/bin/bash
# Check status of database containers

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Database Container Status:"
echo ""

# Check PostgreSQL
if docker ps | grep -q "jaydebeapi-benchmark-postgres"; then
    echo "✓ PostgreSQL: RUNNING"
    if docker-compose exec -T postgres pg_isready -U user -d test_db >/dev/null 2>&1; then
        echo "  Status: Ready to accept connections"
        echo "  Host: localhost:5432"
        echo "  Database: test_db"
    else
        echo "  Status: Starting up..."
    fi
else
    echo "✗ PostgreSQL: NOT RUNNING"
fi

echo ""

# Check MySQL
if docker ps | grep -q "jaydebeapi-benchmark-mysql"; then
    echo "✓ MySQL: RUNNING"
    if docker-compose exec -T mysql mysqladmin ping -h localhost -u user -ppassword --silent >/dev/null 2>&1; then
        echo "  Status: Ready to accept connections"
        echo "  Host: localhost:3306"
        echo "  Database: test_db"
    else
        echo "  Status: Starting up..."
    fi
else
    echo "✗ MySQL: NOT RUNNING"
fi
