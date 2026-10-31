
import pyarrow as pa
from pyarrow.cffi import ffi as arrow_c

# Set by __init__.py after JPype initialization.
# Converts Java SQLException to Python DatabaseError.
_handle_sql_exception = None

# Set by __init__.py during connect() when the extension classes are not on
# the JVM classpath (externally started JVM); None means "resolve from the
# JVM classpath as usual".
_jdbc_utils_class = None


def _get_jdbc_utils():
    """Return the JDBCUtils class of the arrow-jdbc extension.

    Normally resolved from the JVM classpath (where connect() places the
    extension jar when it starts the JVM).  When the JVM was started
    externally without the extension jar, connect() loads it through a
    URLClassLoader and injects the class here so the fetch path keeps
    working with zero classpath configuration.
    """
    if _jdbc_utils_class is not None:
        return _jdbc_utils_class
    import jpype
    return jpype.JClass("org.jaydebeapiarrow.extension.JDBCUtils")


def _import_batch_via_cdata(root):
    """Import a Java VectorSchemaRoot as a PyArrow RecordBatch via C Data Interface."""
    JDBCUtils = _get_jdbc_utils()

    addresses = JDBCUtils.exportNextBatch(root)
    array_ptr = int(addresses[0])
    schema_ptr = int(addresses[1])

    return pa.RecordBatch._import_from_c(array_ptr, schema_ptr)


def convert_jdbc_rs_to_arrow_iterator(rs, batch_size=1024):
    return _get_jdbc_utils().convertResultSetToIterator(rs, batch_size)


def fetch_next_batch(it):
    """
    Fetches the next batch from the ArrowVectorIterator 'it'.
    Returns (rows, schema): a list of rows (tuples) and the batch's Arrow
    schema. Both are empty/None once the iterator is exhausted.

    When the iterator is exhausted, it is automatically closed to release
    the Arrow allocator and JDBC resources.
    """
    if it.hasNext():
        try:
            root = it.next()
        except Exception as e:
            decimal_message = _find_decimal_conversion_message(e)
            if decimal_message:
                raise RuntimeError(decimal_message) from e
            if _handle_sql_exception is not None:
                _handle_sql_exception()
            raise
        try:
            batch = _import_batch_via_cdata(root)
            schema = batch.schema
            rows = [tuple(r.values()) for r in batch.to_pylist()]
            return rows, schema
        finally:
            root.clear()
    else:
        # Iterator exhausted — close to release Arrow allocator and JDBC resources
        try:
            it.close()
        except Exception:
            pass
    return [], None


def _find_decimal_conversion_message(exc):
    current = exc
    while current is not None:
        message = str(current)
        marker = "Could not convert DECIMAL/NUMERIC value"
        if marker in message:
            return message[message.find(marker):]
        try:
            current = current.getCause()
        except AttributeError:
            return None
    return None


def create_pyarrow_batches_from_list(rows):
    if not rows:
        return []

    if not isinstance(rows[0], (list, tuple)):
        # wrap single column values in a list
        rows = [rows, ]

    n_cols = len(rows[0])
    column_wise = [[] for _ in range(n_cols)]

    for r_idx, row in enumerate(rows):
        # Shape Check: Ensure consistency across all rows
        if len(row) != n_cols:
            raise ValueError(
                f"Shape mismatch at row {r_idx}. "
                f"Expected {n_cols} columns, got {len(row)}."
            )

        for c_idx, col in enumerate(row):
            column_wise[c_idx].append(col)

    batch = pa.RecordBatch.from_pydict(
        {f"col_{i}": column_wise[i] for i in range(n_cols)}
    )
    return [batch, ]


def add_pyarrow_batches_to_statement(batches, prepared_statement, is_batch=False):
    JDBCUtils = _get_jdbc_utils()

    if len(batches) == 0:
        return

    reader = pa.RecordBatchReader.from_batches(batches[0].schema, batches)
    c_stream = arrow_c.new("struct ArrowArrayStream*")
    c_stream_ptr = int(arrow_c.cast("uintptr_t", c_stream))
    reader._export_to_c(c_stream_ptr)
    JDBCUtils.prepareStatementFromStream(c_stream_ptr, prepared_statement, is_batch)
