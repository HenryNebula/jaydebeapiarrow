#!/bin/bash
# Stop database containers

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Stopping database containers..."
docker-compose down

echo "✓ Databases stopped"
echo "  To remove data volumes: docker-compose down -v"
