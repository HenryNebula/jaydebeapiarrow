# Benchmark Suite

This directory contains performance benchmarks comparing different methods for fetching data from PostgreSQL through JDBC.

## Overview

The benchmark compares **4 methods** for fetching data:

1. **Psycopg2** - Native Python PostgreSQL adapter (baseline comparison)
2. **Original** - Original `jaydebeapi` implementation using JDBC
3. **Arrow (Drop-in)** - `jaydebeapiarrow` using `fetchall()` (drop-in replacement)
4. **Arrow (Native)** - `jaydebeapiarrow` using zero-copy Arrow batches for optimal performance

## Test Configurations

### Variable Rows Test (default)
Tests performance with increasing row counts:
- **Datasets**: 1M, 5M, 10M rows
- **Columns**: Fixed at 4 columns
- **Command**: `python benchmark/compare_performance.py --test-type rows`

### Variable Columns Test
Tests performance with increasing column counts:
- **Datasets**: 4, 20, 40 columns
- **Rows**: Fixed at 1M rows
- **Command**: `python benchmark/compare_performance.py --test-type columns`

## Prerequisites

### 1. PostgreSQL Database

You need a running PostgreSQL instance with the following configuration:

```bash
# Default connection settings in benchmark scripts
Host: localhost
Port: 5432
Database: test_db
User: user
Password: password
```

To set up the database:

```bash
# Create database and user
createdb test_db
psql -c "CREATE USER user WITH PASSWORD 'password';"
psql -c "GRANT ALL PRIVILEGES ON DATABASE test_db TO user;"
```

### 2. Python Dependencies

Install required packages:

```bash
# From project root
pip install -r dev-requirements.txt
pip install psycopg2 pandas
```

Key dependencies:
- `jpype1` - JVM bridge for JDBC
- `pyarrow` - Apache Arrow support
- `pandas` - Data manipulation
- `psycopg2` - PostgreSQL adapter for baseline comparison
- `jaydebeapi` - Original JDBC wrapper
- `jaydebeapiarrow` - This package (Arrow-accelerated version)

## Running the Benchmarks

### Quick Start (Automated)

The easiest way to run all benchmarks:

```bash
bash benchmark/run_benchmark.sh
```

This script will:
1. Create a fresh virtual environment in `benchmark/.venv_bench`
2. Install all dependencies
3. Download the PostgreSQL JDBC driver
4. Run the variable rows benchmark

### Manual Execution

If you prefer to run benchmarks manually:

```bash
# 1. Download JDBC driver (if not already present)
bash test/download_jdbc_drivers.sh

# 2. Run variable rows benchmark (default)
python benchmark/compare_performance.py

# OR run variable columns benchmark
python benchmark/compare_performance.py --test-type columns
```

### Running Individual Benchmark Modes

You can run specific benchmark modes directly:

```bash
# Baseline Psycopg2
python benchmark/compare_performance.py --mode psycopg2

# Original JayDeBeApi
python benchmark/compare_performance.py --mode original

# Arrow Drop-in (fetchall)
python benchmark/compare_performance.py --mode arrow-tuple

# Arrow Native (zero-copy)
python benchmark/compare_performance.py --mode arrow-native
```

## Benchmark Output

The benchmark runs **3 iterations** per test and reports:

- **Time** - Average execution time across iterations
- **Rows** - Number of rows fetched
- **Speedup** - Performance improvement relative to original `jaydebeapi`

Example output:
```
Dataset      | Method               | Time (s)   | Speedup
----------------------------------------------------------------
1000000      | Psycopg2             | 2.3456     | 5.23x
1000000      | Original             | 12.2654    | 1.00x
1000000      | Arrow (Drop-in)      | 3.1234     | 3.93x
1000000      | Arrow (Native)       | 1.8765     | 6.54x
```

## Files

- **`run_benchmark.sh`** - Automated setup and execution script
- **`compare_performance.py`** - Main benchmark coordinator and worker
- **`prepare_data.py`** - Test data generation utility
- **`download_jdbc_drivers.sh`** - Downloads JDBC drivers (in `test/`)

## Configuration

You can modify benchmark settings in `compare_performance.py`:

```python
JDBC_DRIVER_PATH = "test/jars/postgresql-42.7.2.jar"
JDBC_CLASS = "org.postgresql.Driver"
JDBC_URL = "jdbc:postgresql://localhost:5432/test_db"
DB_USER = "user"
DB_PASS = "password"
QUERY = "SELECT * FROM benchmark_test"
ITERATIONS = 3
```

## Notes

- The **Original** method has a 5-minute timeout per iteration; if exceeded, performance is extrapolated from partial data
- Test data is automatically generated before each benchmark run
- The `benchmark_test` table is dropped and recreated for each test configuration
- All times are reported in seconds
