# Usage

## Installation

```bash
pip install JayDeBeApiArrow
```

Requires Python 3.9+ and a JDK (8+). JPype and PyArrow are installed automatically as dependencies.

## Drop-In Replacement

JayDeBeApiArrow is a **drop-in replacement** for the original [jaydebeapi](https://github.com/baztian/jaydebeapi). If your code uses jaydebeapi today, simply change the import:

```python
# Before
import jaydebeapi

# After
import jaydebeapiarrow
```

Everything else - `connect()`, `execute()`, `fetchall()`, `fetchone()`, `fetchmany()`, `executemany()`, parameter binding, transactions - works identically. No code changes needed.

The drop-in path still benefits from the Arrow fast path under the hood: data is converted to Arrow record batches in-JVM (bypassing the slow row-by-row JPype serialization), then converted to Python tuples for DB-API 2.0 compatibility. This alone gives a **~6.8x speedup** over the original jaydebeapi at 5M rows.

```python
import jaydebeapiarrow

conn = jaydebeapiarrow.connect(
    "org.postgresql.Driver",
    "jdbc:postgresql://host:5432/db",
    ["user", "password"],
    "/path/to/pgjdbc.jar"
)

curs = conn.cursor()
curs.execute("SELECT * FROM large_table")
rows = curs.fetchall()  # standard DB-API - but ~7x faster than jaydebeapi
curs.close()
conn.close()
```

## Native Arrow API

For maximum performance, use the Arrow-native methods to skip the tuple conversion entirely:

```python
with conn.cursor() as curs:
    curs.execute("SELECT * FROM large_table")

    # Streaming - yields pyarrow.RecordBatch objects (memory-efficient for huge results)
    for batch in curs.fetch_arrow_batches():
        print(batch.num_rows)

    # All-at-once - returns a single pyarrow.Table
    table = curs.fetch_arrow_table()

    # Direct to pandas
    df = curs.fetch_df()
```

| Method | Returns | Speedup vs jaydebeapi | Use Case |
|---|---|---|---|
| `fetchall()` / `fetchone()` / `fetchmany()` | `tuple` / `list[tuple]` | ~6.8x | Drop-in replacement, DB-API compatibility |
| `fetch_arrow_batches()` | `Iterator[pyarrow.RecordBatch]` | ~23.7x | Streaming large results |
| `fetch_arrow_table()` | `pyarrow.Table` | ~23.7x | All data at once |
| `fetch_df()` | `pandas.DataFrame` | ~23.7x | Quick path to pandas (requires `pip install jaydebeapiarrow[pandas]`) |

The performance gap between Drop-in and Native grows with column count, because the tuple conversion cost scales linearly with the number of cells. See [Benchmarks](benchmarks.md) for details.

## Connecting

### Basic Connection

```python
conn = jaydebeapiarrow.connect(
    "org.postgresql.Driver",          # JDBC driver class name
    "jdbc:postgresql://host:5432/db", # JDBC connection URL
    ["user", "password"],             # credentials (list or dict)
    "/path/to/pgjdbc.jar"             # driver JAR path(s)
)
```

### Connection Properties (Dict)

```python
conn = jaydebeapiarrow.connect(
    "org.postgresql.Driver",
    "jdbc:postgresql://host:5432/db",
    {
        "user": "user",
        "password": "password",
        "ssl": "true",
        "loginTimeout": "10"
    },
    "/path/to/pgjdbc.jar"
)
```

### Context Manager

```python
with jaydebeapiarrow.connect(
    "org.hsqldb.jdbcDriver",
    "jdbc:hsqldb:mem:.",
    ["SA", ""],
    "/path/to/hsqldb.jar"
) as conn:
    with conn.cursor() as curs:
        curs.execute("SELECT * FROM customers")
        print(curs.fetchall())
```

### `connect()` Parameters

| Parameter | Type | Description |
|---|---|---|
| `jclassname` | `str` | Fully-qualified Java driver class name |
| `url` | `str` | JDBC connection URL |
| `driver_args` | `list` or `dict` or `None` | `[user, password]` or connection properties dict |
| `jars` | `str` or `list[str]` or `None` | Path(s) to JDBC driver JAR(s) |
| `libs` | `str` or `list[str]` or `None` | Path(s) to native libraries |
| `jvm_args` | `list[str]` or `None` | Extra JVM arguments passed to `startJVM()`. Only takes effect on the first `connect()` call (when the JVM is started). Ignored on subsequent calls. |
| `experimental` | `dict` or `None` | Experimental feature flags. See [Experimental Features](#experimental-features). |

## Cursor Methods

### Standard DB-API 2.0

```python
curs = conn.cursor()

# Execute queries
curs.execute("SELECT * FROM users WHERE age > %s", (25,))
curs.execute("INSERT INTO users (name, age) VALUES (?, ?)", ("Alice", 30))

# Batch inserts
curs.executemany(
    "INSERT INTO users (name, age) VALUES (?, ?)",
    [("Bob", 25), ("Carol", 28), ("Dave", 32)]
)

# Fetch results
row = curs.fetchone()          # single tuple or None
rows = curs.fetchmany(100)     # list of tuples
all_rows = curs.fetchall()     # all remaining rows
```

### Arrow-Specific Methods

```python
# Zero-copy Arrow record batches
for batch in curs.fetch_arrow_batches():
    print(batch.num_rows)
    # batch is a pyarrow.RecordBatch

# Single Arrow table (concatenates all batches)
table = curs.fetch_arrow_table()
# table is a pyarrow.Table

# Direct to pandas DataFrame (requires pandas: pip install jaydebeapiarrow[pandas])
df = curs.fetch_df()
# df is a pandas.DataFrame
```

## Cursor Attributes

| Attribute | Description |
|---|---|
| `rowcount` | Number of rows produced/affected by the last `execute*()`. `-1` if no execute has been performed or the count cannot be determined (e.g. SELECT queries). |
| `lastrowid` | The auto-generated key from the last `INSERT` on a table with an identity/auto-increment column. `None` if no key was generated or the table has no identity column. Uses JDBC `getGeneratedKeys()`. |
| `description` | Column metadata for the last query. `None` before execution. |

!!! note "Oracle limitation"
    Oracle JDBC returns ROWID instead of the numeric identity value via `getGeneratedKeys()`. For Oracle, `lastrowid` will always be `None`. Use `RETURNING INTO` for Oracle-specific identity retrieval.

## Parameter Binding

```python
curs.execute("SELECT * FROM users WHERE name = ? AND age > ?", ("Alice", 25))
curs.execute("INSERT INTO events (ts, value) VALUES (?, ?)",
             (datetime(2024, 1, 15, 10, 30), Decimal("99.99")))
```

See [Data Mapping - Parameter Binding](data-mapping.md#parameter-binding) for the full type mapping table.

## Connection Management

```python
# Transactions (autocommit is off by default)
conn.commit()
conn.rollback()

# Close connection
conn.close()

# Connection info
print(conn.jconn)           # underlying Java Connection object
```

## Fork Safety

JPype does not support `fork()` after the JVM has started. JayDeBeApiArrow enforces this with a PID check: if `connect()` detects that the process was forked after JVM startup, it raises `InterfaceError` with an actionable message.

### Thread Safety

The JVM is started exactly once, guarded by a `threading.Lock`. Multiple threads can call `connect()` concurrently - the first thread to acquire the lock starts the JVM; all others wait and then reuse the already-running JVM. Each thread that calls into JPype gets attached to the JVM automatically.

### Process Patterns

| Pattern | Works | Notes |
|---|---|---|
| Single process, multiple threads | Yes | JVM started once, shared across threads |
| `gunicorn --preload` with lazy connections | Yes | Fork happens before JVM start, each worker starts its own JVM |
| `gunicorn` without `--preload` | No | Fork happens after JVM start in parent, PID check fails |
| `multiprocessing` (spawn) | Yes | New process starts fresh, no fork |
| `multiprocessing` (fork) | No | Fork copies JVM state, which is unsafe |

### Workaround: Dynamic Classpath

For forked processes that need to connect, enable the DriverShim pattern:

```python
conn = jaydebeapiarrow.connect(
    "org.postgresql.Driver",
    "jdbc:postgresql://host:5432/db",
    ["user", "password"],
    "/path/to/pgjdbc.jar",
    experimental={"dynamic_classpath": True}
)
```

This bypasses the fork-after-JVM-start guard and loads JDBC drivers dynamically via `URLClassLoader`. Note that shared JVM state from the parent process is still present.

## Experimental Features

### Dynamic Classpath Loading

By default, JPype's classpath is immutable after the JVM starts - you can only load JDBC drivers that were available at JVM startup time.

The `experimental={'dynamic_classpath': True}` flag works around this using the **DriverShim pattern**: new JARs are loaded via Java's `URLClassLoader`, and a shim proxy is registered with `DriverManager` to delegate to the dynamically loaded driver.

!!! warning "Experimental"
    This feature is experimental and may change in future versions.

## Debugging

Enable Java-level debug logging from the JDBC bridge:

```python
import jaydebeapiarrow
jaydebeapiarrow.set_debug(True)

# Now connect and run queries - Java JUL debug messages will appear in stderr
```

## Supported Databases

Any database with a JDBC driver should work. Confirmed compatibility:

| Database | Driver Class |
|---|---|
| PostgreSQL | `org.postgresql.Driver` |
| MySQL | `com.mysql.cj.jdbc.Driver` |
| SQLite (Xerial) | `org.sqlite.JDBC` |
| Oracle | `oracle.jdbc.OracleDriver` |
| SQL Server | `com.microsoft.sqlserver.jdbc.SQLServerDriver` |
| DB2 | `com.ibm.db2.jcc.DB2Driver` |
| HSQLDB | `org.hsqldb.jdbcDriver` |
| Teradata | `com.teradata.jdbc.TeraDriver` |
| Netezza | `org.netezza.Driver` |
| Mimer SQL | `com.mimer.jdbc.Driver` |

## Troubleshooting

### JAVA_HOME not set

```
RuntimeError: Unable to start JVM
```

Set the `JAVA_HOME` environment variable:

```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk
```

### JVM already started (fork issue)

```
InterfaceError: Cannot use jaydebeapiarrow in a forked process.
```

Use `gunicorn --preload` with lazy connections, or enable dynamic classpath - see [Fork Safety](#fork-safety).

### Non-ASCII characters garbled

Add JVM encoding argument or set environment variable:

```python
conn = jaydebeapiarrow.connect(
    "org.postgresql.Driver",
    "jdbc:postgresql://host:5432/db",
    ["user", "password"],
    "/path/to/pgjdbc.jar",
    jvm_args=["-Dfile.encoding=UTF-8"]
)
```

```bash
JAVA_TOOL_OPTIONS="-Dfile.encoding=UTF-8" python your_script.py
```
