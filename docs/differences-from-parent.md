# Differences from Parent JayDeBeApi

This document tracks deviations between this fork (jaydebeapiarrow) and the upstream [JayDeBeApi](https://github.com/baztian/jaydebeapi) (v1.2.3).

## Conversion Behavior Changes

This fork returns native Python types instead of strings for temporal and numeric columns.

| Column Type | Parent | Fork |
|---|---|---|
| `TIMESTAMP` | `str` (via `str(datetime)`) | `datetime.datetime` (naive) |
| `TIME` | `str` (raw Java `toString()`) | `datetime.time` |
| `DATE` | `str` (first 10 chars) | `datetime.date` |
| `DECIMAL` / `NUMERIC` | `float` (or `int` if scale=0) | `decimal.Decimal` (full precision preserved) |
| `BINARY` | `str` (Java object `toString()`) | `memoryview` / `bytes` |
| `TIMESTAMP_WITH_TIMEZONE` | Raw Java object (no dedicated converter) | `datetime.datetime` (timezone-aware, UTC) |

## Features Removed from Parent

- **Python 2 support** — only Python 3 is supported.
- **Jython support** — only CPython + JPype is supported.
- **`_java_sql_blob` / `_java_array_byte` constructors** — `Binary()` now returns `bytes` directly instead of a Java byte array.

## Features Added in Fork

- **Apache Arrow data path** — JDBC data is converted to Arrow record batches in-JVM and streamed to Python, avoiding row-by-row JPype serialization.
- **`fetch_arrow_batches()`** — yields `pyarrow.RecordBatch` objects (zero-copy).
- **`fetch_arrow_table()`** — returns a single `pyarrow.Table`.
- **`fetch_df()`** — returns a `pandas.DataFrame` via the optimized Arrow path.
- **`TIMESTAMP_WITH_TIMEZONE` support** — properly handled as timezone-aware `datetime` (the parent has no converter for this type).
- **`set_debug()`** — enables JUL-level debug logging from the Java bridge.
- **ARRAY type detection** — columns reported as JDBC `ARRAY` are mapped to `VARCHAR` as a degraded fallback with a logged warning. Full ARRAY support is not available in the Arrow JDBC adapter.
- **JSON/JSONB/UUID detection** — columns reported as JDBC `OTHER` with type names containing `JSON` or `UUID` (e.g., PostgreSQL) are explicitly mapped to `VARCHAR`.
- **`bytes`/`bytearray` parameter binding** — the fallback `_to_java()` converter now converts Python bytes to Java `byte[]` for `BLOB`/`BINARY` columns.
