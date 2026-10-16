# Data Mapping

JayDeBeApiArrow converts JDBC data types through two stages: **JDBC to Arrow** (in the JVM) and **Arrow to Python** (on the Python side). This page documents the mapping and known limitations.

## Type Mapping Table

### Numeric Types

| JDBC Type | Arrow Type | Python Type | Notes |
|---|---|---|---|
| `INTEGER` | `Int32` | `int` | |
| `BIGINT` | `Int64` | `int` | |
| `SMALLINT` | `Int16` | `int` | |
| `TINYINT` | `Int8` | `int` | |
| `BOOLEAN` / `BIT` | `Boolean` | `bool` | |
| `FLOAT` / `REAL` | `Float32` | `float` | |
| `DOUBLE` | `Float64` | `float` | |
| `DECIMAL` / `NUMERIC` (precision up to 38) | `Decimal128` | `decimal.Decimal` | Full precision preserved |
| `DECIMAL` / `NUMERIC` (precision 39-76) | `Decimal256` | `decimal.Decimal` | Full precision preserved |
| `DECIMAL` / `NUMERIC` (precision above 76, or unbounded) | `Utf8` | `decimal.Decimal` | String fallback; see [High-Precision Decimals](#high-precision-decimals) |

### String Types

| JDBC Type | Arrow Type | Python Type | Notes |
|---|---|---|---|
| `VARCHAR` | `Utf8` | `str` | |
| `CHAR` / `NCHAR` | `Utf8` | `str` | |
| `NVARCHAR` | `Utf8` | `str` | |
| `LONGVARCHAR` / `LONGNVARCHAR` | `Utf8` | `str` | |
| `CLOB` / `NCLOB` | `Utf8` | `str` | |
| `OTHER` | `Utf8` | `str` | Fallback |

### Temporal Types

| JDBC Type | Arrow Type | Python Type | Notes |
|---|---|---|---|
| `DATE` | `Date32` | `datetime.date` | |
| `TIME` | `Time32` | `datetime.time` | |
| `TIME_WITH_TIMEZONE` | `Utf8` | `str` | Fallback - not natively supported by Arrow |
| `TIMESTAMP` | `Timestamp` | `datetime.datetime` | Naive (no timezone) |
| `TIMESTAMP_WITH_TIMEZONE` | `Timestamp(tz=UTC)` | `datetime.datetime` | Timezone-aware, UTC |

### Binary Types

| JDBC Type | Arrow Type | Python Type | Notes |
|---|---|---|---|
| `BINARY` | `Binary` | `bytes` / `memoryview` | |
| `VARBINARY` | `Binary` | `bytes` / `memoryview` | |
| `BLOB` | `Binary` | `bytes` / `memoryview` | |
| `LONGVARBINARY` | `Binary` | `bytes` / `memoryview` | |

### Special Types

| JDBC Type | Arrow Type | Python Type | Notes |
|---|---|---|---|
| `ARRAY` | `list` | `list` | Nested list per element type (e.g. `[1, 2, 3]` for `INT ARRAY`); row fetch paths only ([#125](https://github.com/HenryNebula/jaydebeapiarrow/issues/125)) |
| `SQLXML` | `Utf8` | `str` | Fallback via `getString()` |
| `ROWID` | `Utf8` | `str` | Fallback |
| `JSON` / `JSONB` / `UUID` / `XML` | `Utf8` | `str` | Detected from type name when reported as `OTHER` |

## How Type Detection Works

The `ExplicitTypeMapper` in `arrow-jdbc-extension.jar` inspects `ResultSetMetaData` to build a per-column type mapping. It handles driver-specific quirks:

1. **Standard types** - JDBC type codes like `Types.INTEGER`, `Types.VARCHAR` are mapped directly
2. **Unknown type codes** - Some drivers use non-standard codes (e.g., Oracle `BINARY_DOUBLE` = 101). The mapper falls back to matching the column type *name* against known patterns
3. **Misreported types** - Some drivers misreport types (e.g., SQLite reports `TIME` as `VARCHAR`). The mapper detects these by comparing type code against type name

## Known Limitations

### Arrow-Unsupported Types

These types are **not natively supported** by the Arrow JDBC adapter. They are converted to `VARCHAR` as a degraded fallback:

- **`SQLXML`** - Returned as string via `getString()`.
- **`ROWID`** - Returned as string.
- **`OTHER`** - Returned as string. Columns with type names containing `JSON`, `UUID`, or `XML` are auto-detected and mapped to `VARCHAR`.
- **`TIME_WITH_TIMEZONE`** - Not natively supported. Falls back to string representation.

### High-Precision Decimals

`DECIMAL` / `NUMERIC` columns are mapped by declared precision:

- Up to 38 digits: Arrow `decimal128`
- 39 to 76 digits: Arrow `decimal256`
- Above 76 digits, or with no declared precision (e.g. PostgreSQL `numeric`): transferred as `Utf8`, because no Arrow decimal type can hold the value

String-fallback columns carry `jdbc_type` / `jdbc_precision` / `jdbc_scale` field metadata. All fetch paths restore exact `decimal.Decimal` values from these strings, so decimals round-trip with full precision at any width. In the Arrow-native paths (`fetch_arrow_batches`, `fetch_arrow_table`) fallback columns stay `utf8` with the metadata attached; `fetch_arrow_table` additionally casts a column back to `decimal256` when the returned data fits within 76 digits.

`DECIMAL` / `NUMERIC` ARRAY columns use the same fallback: element precision is not available from JDBC metadata, so elements always transfer as strings. The row fetch paths (`fetchone`, `fetchmany`, `fetchall`) restore every element to an exact `decimal.Decimal` — including values beyond decimal256 — and preserve NULL arrays and NULL elements ([#121](https://github.com/HenryNebula/jaydebeapiarrow/issues/121)). ARRAY columns are not yet supported on the Arrow-native paths ([#125](https://github.com/HenryNebula/jaydebeapiarrow/issues/125)).

### Driver-Specific Quirks

#### SQLite

- **TIME type**: SQLite JDBC reports `TIME` columns as `VARCHAR`. This is auto-detected and corrected.
- **NUMERIC/DECIMAL precision**: SQLite stores all numeric values as doubles internally, so precision information from `ResultSetMetaData` is unreliable.

#### Oracle

- **BINARY_DOUBLE**: Oracle JDBC reports this with type code 101 (non-standard). Auto-detected by type name matching.
- **TIMESTAMP WITH TIME ZONE**: Type code varies between `ojdbc8` (101) and `ojdbc11` (2013). Auto-detected by type name matching.
- **BIGINT**: Not supported natively by some Oracle JDBC driver versions.

#### PostgreSQL

- **TIMESTAMPTZ**: Reported as `Types.TIMESTAMP` by the PostgreSQL JDBC driver. Auto-detected by checking if the type name is `"timestamptz"` and overriding to `TIMESTAMP_WITH_TIMEZONE`.

### Parameter Binding

| Python Type | JDBC Type | Example |
|---|---|---|
| `str` | `VARCHAR` | `"hello"` |
| `int` | `INTEGER` | `42` |
| `float` | `DOUBLE` | `3.14` |
| `bool` | `BOOLEAN` | `True` |
| `decimal.Decimal` | `DECIMAL` | `Decimal("10.50")` |
| `datetime.datetime` | `TIMESTAMP` | `datetime(2024, 1, 15, 10, 30)` |
| `datetime.date` | `DATE` | `date(2024, 1, 15)` |
| `datetime.time` | `TIME` | `time(10, 30, 0)` |
| `bytes` / `bytearray` | `BINARY` | `b"\x00\x01\x02"` |
| `None` | `NULL` | `None` (uses `setNull()` internally) |
| `list` | `ARRAY` | `[1, 2, 3]` converted to Java array |

`None` parameters are handled via JDBC `setNull()` rather than `setObject(i, null)` for driver compatibility (e.g., Teradata rejects `setObject` with null).

`list` parameters are converted to Java arrays (`int[]`, `String[]`, etc.) and bound via `setObject()`. The ARRAY column type is supported for both reading and writing with drivers that implement `java.sql.Array`.

## Comparison with Parent JayDeBeApi

| Column Type | Parent (jaydebeapi) | Fork (jaydebeapiarrow) |
|---|---|---|
| `TIMESTAMP` | `str` | `datetime.datetime` (naive) |
| `TIME` | `str` | `datetime.time` |
| `DATE` | `str` | `datetime.date` |
| `DECIMAL` / `NUMERIC` | `float` / `int` | `decimal.Decimal` (full precision) |
| `BINARY` | `str` | `bytes` / `memoryview` |
| `TIMESTAMP_WITH_TIMEZONE` | Raw Java object | `datetime.datetime` (timezone-aware) |
| `ARRAY` | Raw Java object | `list` (read and write) |

See [Differences](differences.md) for the full list of changes from the parent project.
