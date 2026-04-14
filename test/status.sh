#!/bin/bash
# Check status of database containers

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Database Container Status:"
echo ""

check_status() {
  local name="$1"
  local container="$2"
  if docker ps | grep -q "$container"; then
      echo "$name: RUNNING"
  else
      echo "$name: NOT RUNNING"
  fi
  echo ""
}

check_status "PostgreSQL" "jaydebeapi-benchmark-postgres"
check_status "MySQL" "jaydebeapi-benchmark-mysql"
check_status "Oracle XE" "jaydebeapi-test-oracle"
check_status "MS SQL Server" "jaydebeapi-test-mssql"
check_status "IBM DB2" "jaydebeapi-test-db2"
check_status "Apache Drill" "jaydebeapi-test-drill"
check_status "Trino" "jaydebeapi-test-trino"
