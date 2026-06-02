# JayDeBeApiArrow - High-Performance JDBC to Python DB-API Bridge

[![Test Status](https://github.com/HenryNebula/jaydebeapiarrow/actions/workflows/tests.yml/badge.svg)](https://github.com/HenryNebula/jaydebeapiarrow/actions/workflows/tests.yml)
[![PyPI version](https://img.shields.io/pypi/v/JayDeBeApiArrow.svg)](https://pypi.python.org/pypi/JayDeBeApiArrow/)

The **JayDeBeApiArrow** module allows you to connect from Python code to databases using Java [JDBC](http://java.sun.com/products/jdbc/overview.html). It provides a Python [DB-API v2.0](http://www.python.org/dev/peps/pep-0249/) to that database.

> **Note:** This is a fork of the original [JayDeBeApi](https://github.com/baztian/jaydebeapi) project.

## Key Differences in this Fork

1.  **High Performance with Apache Arrow:**
    The primary goal of this fork is to significantly improve data fetch performance. Instead of iterating through JDBC ResultSets row-by-row in Python (which has high overhead), this library uses a custom Java extension (`arrow-jdbc-extension`) to convert JDBC data into **Apache Arrow** record batches directly within the JVM. These batches are then efficiently transferred to Python.

2.  **Modernization:**
    *   **Python 3 Only:** Support for Python 2 has been removed.
    *   **JPype Only:** Support for Jython has been removed to focus on the CPython + JPype architecture.
    *   **Strict Typing:** Enforces stricter typing for Decimal and temporal types.

It works on ordinary Python (cPython) using the [JPype](https://pypi.python.org/pypi/JPype1/) Java integration.

## Install

You can get and install JayDeBeApiArrow with pip:

```bash
pip install JayDeBeApiArrow
```

Or with uv:

```bash
uv add JayDeBeApiArrow
```

Or install from source:

```bash
git clone https://github.com/HenryNebula/jaydebeapiArrow.git
cd jaydebeapiArrow
pip install -e .
```

## Usage

Basically you just import the `jaydebeapiarrow` Python module and execute the `connect` method. This gives you a DB-API conform connection to the database.

The first argument to `connect` is the name of the Java driver class. The second argument is a string with the JDBC connection URL. Third you can optionally supply a sequence consisting of user and password or alternatively a dictionary containing arguments that are internally passed as properties to the Java `DriverManager.getConnection` method. See the Javadoc of `DriverManager` class for details.

The next parameter to `connect` is optional as well and specifies the jar-Files of the driver if your classpath isn't set up sufficiently yet. The classpath set in `CLASSPATH` environment variable will be honored.

Here is an example:

```python
import jaydebeapiarrow
conn = jaydebeapiarrow.connect(
    "org.hsqldb.jdbcDriver",
    "jdbc:hsqldb:mem:.",
    ["SA", ""],
    "/path/to/hsqldb.jar"
)
curs = conn.cursor()
curs.execute('create table CUSTOMER'
             '("CUST_ID" INTEGER not null,'
             ' "NAME" VARCHAR(50) not null,'
             ' primary key ("CUST_ID"))')
curs.execute("insert into CUSTOMER values (?, ?)", (1, 'John'))
curs.execute("select * from CUSTOMER")
print(curs.fetchall())
# Output: [(1, 'John')]
curs.close()
conn.close()
```

If you're having trouble getting this work check if your `JAVA_HOME` environment variable is set correctly. For example:

```bash
JAVA_HOME=/usr/lib/jvm/java-8-openjdk python
```

An alternative way to establish connection using connection properties:

```python
conn = jaydebeapiarrow.connect(
    "org.hsqldb.jdbcDriver",
    "jdbc:hsqldb:mem:.",
    {
        'user': "SA", 'password': "",
        'other_property': "foobar"
    },
    "/path/to/hsqldb.jar"
)
```

Also using the `with` statement might be handy:

```python
with jaydebeapiarrow.connect(
    "org.hsqldb.jdbcDriver",
    "jdbc:hsqldb:mem:.",
    ["SA", ""],
    "/path/to/hsqldb.jar"
) as conn:
    with conn.cursor() as curs:
        curs.execute("select count(*) from CUSTOMER")
        print(curs.fetchall())
        # Output: [(1,)]
```

## Supported Databases

In theory *every database with a suitable JDBC driver should work*. It is confirmed to work with the following databases:

*   SQLite
*   Hypersonic SQL (HSQLDB)
*   IBM DB2
*   IBM DB2 for mainframes
*   Oracle
*   Teradata DB
*   Netezza
*   Mimer DB
*   Microsoft SQL Server
*   MySQL
*   PostgreSQL
*   ...and many more.

## Testing

Integration tests are located in `test/`. Tests run via [pytest](https://docs.pytest.org/) and cover all supported databases: SQLite (in-memory), HSQLDB, PostgreSQL, MySQL, MSSQL, Oracle, DB2, Trino, and Apache Drill.

### Build JARs and download drivers

```bash
uv run bash test/build.sh                 # Build arrow-jdbc-extension and MockDriver JARs
uv run bash test/download_jdbc_drivers.sh # Download JDBC drivers
```

### Run tests

```bash
CLASSPATH="test/jars/*:test/mock-jars/*" uv run pytest test/test_mock.py test/test_infrastructure.py -v   # Mock + infrastructure
CLASSPATH="test/jars/*" uv run pytest test/test_hsqldb.py -v                                                # HSQLDB
CLASSPATH="test/jars/*" uv run pytest test/test_sqlite.py::SqliteXerialTest -v                              # SQLite JDBC
CLASSPATH="test/jars/*" uv run pytest test/ -v --tb=short                                                  # All tests
```

Pytest is configured in `pyproject.toml` to run tests in parallel across files using `pytest-xdist` with `--dist loadfile`.

### External database tests

Container-based databases are managed via Docker Compose:

```bash
# Start all databases
cd test && docker compose up -d

# Check status
cd test && docker compose ps

# Stop all databases
cd test && docker compose down
```

Database connection defaults (overridable via environment variables):

| Database | Host | Port | DB | User | Password | Env prefix |
|---|---|---|---|---|---|---|
| PostgreSQL | localhost | 15432 | test_db | user | password | `JY_PG_*` |
| MySQL | localhost | 13306 | test_db | user | password | `JY_MYSQL_*` |
| MSSQL | localhost | 11433 | — | sa | Password123! | `JY_MSSQL_*` |
| Oracle | localhost | 11521 | XEPDB1 | system | Password123! | `JY_ORACLE_*` |
| DB2 | localhost | 15000 | test_db | db2inst1 | Password123! | `JY_DB2_*` |
| Trino | localhost | 18080 | — | test | — | `JY_TRINO_*` |
| Drill | localhost | 31010 | — | — | — | `JY_DRILL_*` |

## Benchmarks

This approach was inspired by [Uwe Korn's work on pyarrow.jvm](https://uwekorn.com/2019/11/17/fast-jdbc-access-in-python-using-pyarrow-jvm.html) (Apache Drill) and [Razvi Noorul's Trino benchmarks](https://medium.com/@noorulrazvi/trino-jdbc-access-in-python-using-pyarrow-jvm-d1b75fe039ee), both demonstrating 100x+ speedups by using Arrow to bypass JPype's row-by-row serialization.

Our benchmarks (local PostgreSQL, 5M rows, 4 columns) show a **23.7x speedup** over plain jaydebeapi using the Native Arrow API. The difference in multiplier vs the referenced posts is due to methodology: they tested against distributed query engines (Drill, Trino) over network connections with higher per-row JDBC overhead. PostgreSQL's JDBC driver is fast at row retrieval, so the baseline is lower. The absolute Arrow throughput is comparable across all three.

The reading path uses the Arrow **C Data Interface** (`Data.exportVectorSchemaRoot` → `pa.RecordBatch._import_from_c`), which bypasses `pyarrow.jvm` entirely. This brings the Native Arrow API to within **6% of psycopg2**, a native C driver.

| Method | 5M rows | Throughput | vs jaydebeapi |
|---|---|---|---|
| jaydebeapi (baseline) | 180.1s | 28K rows/s | — |
| Drop-in replacement | 26.5s | 189K rows/s | 6.8x |
| Native Arrow API (C Data Interface) | 7.6s | 658K rows/s | **23.7x** |
| Psycopg2 (native driver) | 7.2s | 694K rows/s | 25.0x |

See `benchmark/` for scripts to reproduce these results.

## Contributing

Please submit bugs and patches to the [JayDeBeApiArrow issue tracker](https://github.com/HenryNebula/jaydebeapiArrow/issues). All contributors will be acknowledged. Thanks!

## License

JayDeBeApiArrow is released under the GNU Lesser General Public license (LGPL). See the file `COPYING` and `COPYING.LESSER` in the distribution for details.
