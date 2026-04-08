# Benchmark Database Setup

This directory contains Docker configurations for running PostgreSQL and MySQL databases for benchmarking.

## Quick Start

### Start All Databases
```bash
cd benchmark/setup
./start.sh
```

### Start Specific Database
```bash
cd benchmark/setup
./start.sh postgres   # Only PostgreSQL
./start.sh mysql      # Only MySQL
```

### Check Status
```bash
cd benchmark/setup
./status.sh
```

### Stop Databases
```bash
cd benchmark/setup
./stop.sh
```

## Database Connection Details

### PostgreSQL
- **Host**: `localhost:5432`
- **Database**: `test_db`
- **User**: `user`
- **Password**: `password`

### MySQL
- **Host**: `localhost:3306`
- **Database**: `test_db`
- **User**: `user`
- **Password**: `password`
- **Root Password**: `rootpassword`

## Running Benchmarks

After starting the databases, you can run the benchmarks from the project root:

```bash
# Activate virtual environment
source .venv/bin/activate

# Run PostgreSQL benchmarks
python benchmark/compare_performance.py --test-type rows

# Run MySQL benchmarks (if implemented)
python benchmark/compare_performance.py --test-type rows --db mysql
```

## Data Persistence

Database data is stored in Docker volumes:
- `postgres_data` - PostgreSQL data
- `mysql_data` - MySQL data

To completely reset the databases (remove all data):
```bash
cd benchmark/setup
docker-compose down -v
./start.sh
```

## Requirements

- Docker
- Docker Compose

## Troubleshooting

### Port Already in Use
If you get "port already in use" errors:
```bash
# Check what's using the port
lsof -i :5432  # PostgreSQL
lsof -i :3306  # MySQL

# Stop conflicting services or change ports in docker-compose.yml
```

### Container Won't Start
```bash
# Check logs
docker-compose logs postgres
docker-compose logs mysql

# Restart containers
docker-compose restart
```

### Reset Everything
```bash
# Stop and remove all containers and volumes
docker-compose down -v

# Start fresh
./start.sh
```
