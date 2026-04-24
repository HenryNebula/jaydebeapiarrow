import sys, traceback
import tempfile
from itertools import islice

import pyarrow as pa
from pyarrow.cffi import ffi as arrow_c


def convert_jdbc_rs_to_arrow_iterator(rs, batch_size=1024):
    import jpype.imports
    from org.jaydebeapiarrow.extension import JDBCUtils

    return JDBCUtils.convertResultSetToIterator(rs, batch_size)


def fetch_next_batch(it):
    """
    Fetches the next batch from the ArrowVectorIterator 'it'.
    Returns a list of rows (tuples).
    Returns empty list if iterator is exhausted.

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
            raise
        try:
            batch = pa.jvm.record_batch(root).to_pylist()
            rows = [tuple(r.values()) for r in batch]
            return rows
        finally:
            root.clear()
    else:
        # Iterator exhausted — close to release Arrow allocator and JDBC resources
        try:
            it.close()
        except Exception:
            pass
    return []


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


def read_rows_from_arrow_iterator(it, nrows=-1):
    rows = []

    nrows_remaining = nrows

    try:
        for root in it:
            if root is None:
                break
            try:
                batch = pa.jvm.record_batch(root).to_pylist()
                _rows = [tuple(r.values()) for r in batch]
                if nrows_remaining > 0:
                    _rows = _rows[:min(len(_rows), nrows_remaining)]
                    nrows_remaining -= len(_rows)
                else:
                    if nrows > 0:
                        break
                rows.extend(_rows)
            finally:
                root.clear()

    except Exception as e:
        traceback.print_exc()
        print(f"Error converting iterator to rows: {e}")
        raise e

    finally:
        # Close iterator to release Arrow allocator and JDBC resources
        try:
            it.close()
        except Exception:
            pass

    if nrows > 0:
        assert nrows >= len(rows), f"Mismatched number rows: {len(rows)} (expected {nrows})"
    return rows


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
        {"col_{}".format(i): column_wise[i] for i in range(n_cols)}
    )
    return [batch, ]


def add_pyarrow_batches_to_statement(batches, prepared_statement, is_batch=False):
    import jpype.imports
    from org.jaydebeapiarrow.extension import JDBCUtils

    if len(batches) == 0:
        return

    reader = pa.RecordBatchReader.from_batches(batches[0].schema, batches)
    c_stream = arrow_c.new("struct ArrowArrayStream*")
    c_stream_ptr = int(arrow_c.cast("uintptr_t", c_stream))
    reader._export_to_c(c_stream_ptr)
    JDBCUtils.prepareStatementFromStream(c_stream_ptr, prepared_statement, is_batch)
