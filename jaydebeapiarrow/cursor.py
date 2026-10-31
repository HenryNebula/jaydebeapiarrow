# DB-API 2.0 Cursor object and the DECIMAL/NUMERIC fallback helpers it uses
# for columns that exceed Arrow's decimal256 capacity.  Extracted from
# __init__.py; re-exported there so `import jaydebeapiarrow` keeps working.

import datetime
from decimal import Decimal

import jaydebeapiarrow._bridge as _bridge
from jaydebeapiarrow._dbapi import DBAPITypeObject, Error, ProgrammingError
from jaydebeapiarrow.lib.arrow_utils import (
    _import_batch_via_cdata,
    add_pyarrow_batches_to_statement,
    convert_jdbc_rs_to_arrow_iterator,
    create_pyarrow_batches_from_list,
    fetch_next_batch,
)


# DB-API 2.0 Cursor Object
def _decimal_field_metadata(jdbc_type_name, precision, scale):
    """Arrow field metadata marking a utf8 column as a DECIMAL/NUMERIC that
    exceeded Arrow's decimal256 capacity (or had no declared precision), so
    downstream consumers can recover its intended type."""
    return {
        "jdbc_type": str(jdbc_type_name),
        "jdbc_precision": str(int(precision)),
        "jdbc_scale": str(int(scale)),
    }


def _is_decimal_array_type_name(type_name):
    """True for JDBC ARRAY type names whose elements are DECIMAL/NUMERIC.
    Mirrors ExplicitTypeMapper.inferElementJdbcType's decimal branch, which
    routes these to VARCHAR elements (element precision is unknowable)."""
    upper = type_name.upper().strip()
    return ("DECIMAL ARRAY" in upper or "NUMERIC ARRAY" in upper
            or upper in ("DECIMAL[]", "NUMERIC[]", "_NUMERIC", "_DECIMAL"))


def _decimal_fallback_indexes(schema, decimal_meta):
    """Map schema field index -> (jdbc_type_name, precision, scale) for
    fields that arrived as utf8 (or list<utf8> for DECIMAL arrays) but were
    DECIMAL/NUMERIC in JDBC metadata."""
    import pyarrow as pa

    def is_stringy(dtype):
        return (pa.types.is_string(dtype)
                or pa.types.is_large_string(dtype))

    fallback = {}
    for index, jdbc_type_name, precision, scale in decimal_meta:
        if index < len(schema):
            dtype = schema.field(index).type
            if is_stringy(dtype):
                fallback[index] = (jdbc_type_name, precision, scale)
            elif (pa.types.is_list(dtype)
                    and is_stringy(dtype.value_type)):
                # DECIMAL ARRAY transferred with utf8 elements (#121)
                fallback[index] = (jdbc_type_name, precision, scale)
    return fallback


def _infer_decimal_widths(strings):
    """Scan decimal strings and return (max_total_digits, max_scale) — the
    smallest decimal shape that holds every value."""
    max_digits = 0
    max_scale = 0
    for value in strings:
        if value is None:
            continue
        if "E" in value or "e" in value:
            # BigDecimal.toString() uses scientific notation, e.g. "1E-7"
            t = Decimal(value).as_tuple()
            # A positive exponent pads the integer part ("1E+7" = 8 digits).
            digits = len(t.digits) + max(0, t.exponent)
            scale = max(0, -t.exponent)
        else:
            int_part, _, frac = value.partition(".")
            digits = len(int_part.lstrip("-")) + len(frac)
            scale = len(frac)
        if digits > max_digits:
            max_digits = digits
        if scale > max_scale:
            max_scale = scale
    return max_digits, max_scale


class Cursor:

    _rs = None
    _meta = None
    _description = None
    _iter = None
    _buffer = None
    _decimal_cols = None
    _decimal_info = None

    def __init__(self, connection):
        self._connection = connection
        self._buffer = []
        self._prep = None
        self.rowcount = -1
        self.lastrowid = None
        # Whether execute()/executemany() has ever run on this cursor;
        # fetch methods raise ProgrammingError before the first execute
        # (PEP 249) but stay lenient after a statement that produced no
        # result set.
        self._executed = False

    @property
    def connection(self):
        return self._connection

    @property
    def description(self):
        if self._description:
            return self._description
        m = self._meta
        if m:
            count = m.getColumnCount()
            self._description = []
            for col in range(1, count + 1):
                size = m.getColumnDisplaySize(col)
                jdbc_type = m.getColumnType(col)
                if jdbc_type == 0:
                    # PEP-0249: SQL NULL values are represented by the
                    # Python None singleton
                    dbapi_type = None
                else:
                    dbapi_type = DBAPITypeObject._map_jdbc_type_to_dbapi(jdbc_type)
                col_desc = ( m.getColumnLabel(col),
                             dbapi_type,
                             size,
                             size,
                             m.getPrecision(col),
                             m.getScale(col),
                             m.isNullable(col),
                             )
                self._description.append(col_desc)
            return self._description

#   optional callproc(self, procname, *parameters) unsupported

    def close(self):
        self._close_last()
        self._connection = None

    def _close_last(self):
        """Close the resultset and reset collected meta data.
        """
        if self._iter:
            try:
                self._iter.close()
            except Exception:
                pass
        self._iter = None
        self._buffer = []
        if self._rs:
            self._rs.close()
        self._rs = None
        if self._prep:
            self._prep.close()
        self._prep = None
        self._meta = None
        self._description = None
        self._decimal_cols = None
        self._decimal_info = None

    def _stringify_params(self, params, is_batch):
        if not params:
            return params

        def _to_str(x):
            if isinstance(x, (datetime.date, datetime.time, datetime.datetime)):
                return str(x)
            return x

        if is_batch:
            # params is a sequence of sequences
            return [[_to_str(p) for p in row] for row in params]
        else:
            # params is a sequence
            return [_to_str(p) for p in params]

    def _set_stmt_parms(self, statement, parameters, is_batch=False):
        if self._connection._stringify_dates:
             parameters = self._stringify_params(parameters, is_batch)
        try:
            batches = create_pyarrow_batches_from_list(parameters)
            add_pyarrow_batches_to_statement(batches, statement, is_batch=is_batch)
        except Exception:
            self._set_stmt_parms_fallback(statement, parameters, is_batch)

    def _set_stmt_parms_fallback(self, statement, parameters, is_batch=False):
        """Fallback using standard JDBC setObject() for drivers that don't
        support Arrow-stream parameter binding (e.g. Trino)."""
        import jpype

        Types_NULL = jpype.java.sql.Types.NULL

        def _to_java(p):
            """Convert Python types to Java SQL types for setObject()."""
            if p is None:
                return p  # JPype passes Python None as Java null; setObject(i, null) is valid JDBC
            if isinstance(p, bool):
                return p
            if isinstance(p, (bytes, bytearray)):
                return jpype.JArray(jpype.JByte)(p)
            if isinstance(p, datetime.datetime):
                return jpype.JClass("java.sql.Timestamp").valueOf(
                    p.strftime("%Y-%m-%d %H:%M:%S.%f"))
            if isinstance(p, datetime.date):
                return jpype.JClass("java.sql.Date").valueOf(p.isoformat())
            if isinstance(p, datetime.time):
                return jpype.JClass("java.sql.Time").valueOf(
                    p.strftime("%H:%M:%S"))
            if isinstance(p, Decimal):
                return jpype.JClass("java.math.BigDecimal")(str(p))
            if isinstance(p, list):
                return _list_to_java_array(p)
            return p

        def _list_to_java_array(lst):
            """Convert a Python list to a Java array for setArray() binding."""
            if not lst:
                return jpype.JArray(jpype.JString)(0)

            # Infer element type from first non-None element
            sample = None
            for item in lst:
                if item is not None:
                    sample = item
                    break

            if sample is None:
                return jpype.JArray(jpype.JString)([None] * len(lst))
            if isinstance(sample, bool):
                return jpype.JArray(jpype.JBoolean)(lst)
            if isinstance(sample, int):
                return jpype.JArray(jpype.JInt)(lst)
            if isinstance(sample, float):
                return jpype.JArray(jpype.JDouble)(lst)
            if isinstance(sample, str):
                return jpype.JArray(jpype.JString)(lst)
            if isinstance(sample, Decimal):
                return jpype.JArray(jpype.JString)([str(x) for x in lst])

            return jpype.JArray(jpype.JString)([str(x) for x in lst])

        def _bind_param(stmt, idx, p):
            """Bind a parameter, using setArray for list values."""
            if isinstance(p, list):
                java_arr = _list_to_java_array(p)
                try:
                    conn = stmt.getConnection()
                    sql_type = _infer_sql_type_name(java_arr)
                    sql_array = conn.createArrayOf(sql_type, java_arr)
                    stmt.setArray(idx, sql_array)
                except Exception:
                    stmt.setObject(idx, java_arr)
            else:
                stmt.setObject(idx, _to_java(p))

        def _infer_sql_type_name(java_arr):
            """Map a Java array class to its SQL type name for createArrayOf()."""
            import jpype
            cls = java_arr.getClass().getComponentType()
            if cls == jpype.JBoolean:
                return "BOOLEAN"
            if cls == jpype.JInt:
                return "INTEGER"
            if cls == jpype.JDouble:
                return "DOUBLE"
            if cls == jpype.JString:
                return "VARCHAR"
            return "VARCHAR"

        if is_batch:
            for row in parameters:
                for i, p in enumerate(row):
                    if p is None:
                        statement.setNull(i + 1, Types_NULL)
                    elif isinstance(p, list):
                        _bind_param(statement, i + 1, p)
                    else:
                        statement.setObject(i + 1, _to_java(p))
                statement.addBatch()
        else:
            for i, p in enumerate(parameters):
                if p is None:
                    statement.setNull(i + 1, Types_NULL)
                elif isinstance(p, list):
                    _bind_param(statement, i + 1, p)
                else:
                    statement.setObject(i + 1, _to_java(p))

    def execute(self, operation, parameters=None):
        if self._connection._closed:
            raise Error()
        if not parameters:
            parameters = ()
        self._close_last()
        self.lastrowid = None
        self._executed = True
        try:
            self._prep = self._connection.jconn.prepareStatement(operation, 1)
        except Exception:
            try:
                self._prep = self._connection.jconn.prepareStatement(operation)
            except Exception:
                _bridge._handle_sql_exception()
        self._set_stmt_parms(self._prep, parameters, is_batch=False)
        try:
            is_rs = self._prep.execute()
        except Exception:
            _bridge._handle_sql_exception()
        if is_rs:
            self._rs = self._prep.getResultSet()
            self._meta = self._rs.getMetaData()
            self.rowcount = -1
        else:
            self.rowcount = self._prep.getUpdateCount()
            try:
                gk_rs = self._prep.getGeneratedKeys()
                if gk_rs.next():
                    self.lastrowid = int(gk_rs.getLong(1))
                    if gk_rs.wasNull():
                        self.lastrowid = None
            except Exception:
                pass


    def executemany(self, operation, seq_of_parameters):
        self._close_last()
        self.lastrowid = None
        self._executed = True
        try:
            self._prep = self._connection.jconn.prepareStatement(operation, 1)
        except Exception:
            try:
                self._prep = self._connection.jconn.prepareStatement(operation)
            except Exception:
                _bridge._handle_sql_exception()
        self._set_stmt_parms(self._prep, seq_of_parameters, is_batch=True)
        try:
            update_counts = self._prep.executeBatch()
        except Exception:
            _bridge._handle_sql_exception()
        # Some drivers return null instead of an int[] (e.g. the
        # Mockito-backed MockDriver's unstubbed executeBatch); PEP 249 says
        # rowcount is -1 when the count is not determinable by the interface.
        if update_counts is None:
            self.rowcount = -1
        else:
            self.rowcount = sum(update_counts)
        self._close_last()

    def _get_iter(self):
        if self._iter:
            return self._iter
        if not self._rs:
            raise Error()
        # Use a reasonable batch size.
        # For small reads (fetchone), this might be overhead, but it's safe.
        # For large reads (fetchall), this is efficient.
        # Using arraysize or a default.
        batch_size = max(self.arraysize, 1024)
        self._iter = convert_jdbc_rs_to_arrow_iterator(self._rs, batch_size=batch_size)
        return self._iter

    def _jdbc_decimal_info(self):
        """Cached (0-based index, jdbc type name, precision, scale) tuples
        for the result set's DECIMAL/NUMERIC columns, plus DECIMAL ARRAY
        columns (whose elements transfer as strings; precision/scale are
        unknowable for array elements, so 0/0 is reported)."""
        if self._decimal_info is None:
            rows = []
            if self._meta is not None:
                import jpype
                types = jpype.java.sql.Types
                decimal_consts = (int(types.DECIMAL), int(types.NUMERIC))
                array_const = int(types.ARRAY)
                m = self._meta
                for col in range(1, m.getColumnCount() + 1):
                    jdbc_type = int(m.getColumnType(col))
                    type_name = str(m.getColumnTypeName(col))
                    if jdbc_type in decimal_consts:
                        rows.append((col - 1,
                                     type_name,
                                     int(m.getPrecision(col)),
                                     int(m.getScale(col))))
                    elif (jdbc_type == array_const
                            and _is_decimal_array_type_name(type_name)):
                        rows.append((col - 1, type_name, 0, 0))
            self._decimal_info = tuple(rows)
        return self._decimal_info

    def _restore_decimal_columns(self, rows, schema=None):
        """Rebuild Decimals for DECIMAL/NUMERIC columns that arrived as
        UTF-8 strings (beyond decimal256 capacity, or unbounded precision),
        including elements of DECIMAL ARRAY columns (#121)."""
        if self._decimal_cols is None and schema is not None:
            # Detect once from the batch schema — the ground truth of what
            # the Java type mapping produced — instead of sniffing values,
            # so nullable decimal128 columns are never falsely flagged.
            self._decimal_cols = tuple(_decimal_fallback_indexes(
                schema, self._jdbc_decimal_info()))
        if not self._decimal_cols or not rows:
            return rows
        cols = [i for i in self._decimal_cols if i < len(rows[0])]
        out = []
        for row in rows:
            row = list(row)
            for i in cols:
                v = row[i]
                if isinstance(v, str):
                    row[i] = Decimal(v)
                elif isinstance(v, list):
                    row[i] = [Decimal(e) if isinstance(e, str) else e
                              for e in v]
            out.append(tuple(row))
        return out

    def _verify_fetchable(self, method_name):
        """PEP 249: fetch methods raise an Error when no execute() has been
        made on the cursor.  A statement that was executed but produced no
        result set (DDL/DML, executemany) keeps the established lenient
        None/[] behavior — see the fetch_after_ddl tests in test_hsqldb."""
        if not self._rs and not self._executed:
            raise ProgrammingError(
                f'{method_name}() called before execute()')

    def fetchone(self):
        self._verify_fetchable('fetchone')
        if not self._rs:
            return None

        if self._buffer:
            return self._buffer.pop(0)

        it = self._get_iter()
        rows, schema = fetch_next_batch(it)
        rows = self._restore_decimal_columns(rows, schema)
        if rows:
            self._buffer.extend(rows)
            return self._buffer.pop(0)

        # Iterator exhausted and closed by fetch_next_batch
        self._iter = None
        return None

    def fetchmany(self, size=None):
        self._verify_fetchable('fetchmany')
        if not self._rs:
            return []

        if size is None:
            size = self.arraysize

        assert size > 0, f"Fetchmany expects positive size other than size={size}."

        result = []
        while len(result) < size:
            if self._buffer:
                needed = size - len(result)
                take = self._buffer[:needed]
                self._buffer = self._buffer[needed:]
                result.extend(take)
            else:
                it = self._get_iter()
                rows, schema = fetch_next_batch(it)
                rows = self._restore_decimal_columns(rows, schema)
                if not rows:
                    # Iterator exhausted and closed by fetch_next_batch
                    self._iter = None
                    break
                self._buffer.extend(rows)

        return result

    def fetchall(self):
        self._verify_fetchable('fetchall')
        if not self._rs:
            return []

        result = []
        if self._buffer:
            result.extend(self._buffer)
            self._buffer = []

        it = self._get_iter()

        # We can implement a more efficient fetchall if we want to avoid python loops for buffering,
        # but reusing fetch_next_batch is simpler.
        while True:
            rows, schema = fetch_next_batch(it)
            rows = self._restore_decimal_columns(rows, schema)
            if not rows:
                break
            result.extend(rows)

        # Iterator exhausted and closed by fetch_next_batch
        self._iter = None
        return result

    # optional nextset() unsupported

    arraysize = 1

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass

    def fetch_arrow_batches(self):
        """
        Fetch results as Arrow RecordBatches (zero-copy, native Arrow format).

        This is the most efficient way to retrieve data for Arrow-native workflows.
        Returns a generator that yields pyarrow.RecordBatch objects.

        Example:
            for batch in cursor.fetch_arrow_batches():
                # Process Arrow batch directly (zero-copy)
                df = batch.to_pandas()  # If you need pandas
                # OR: process with any Arrow-compatible library

        Returns:
            Generator[pyarrow.RecordBatch]: Arrow RecordBatches from the query result

        Note:
            This is significantly faster (3-4x) than fetchall() for Arrow-native workflows
            because it avoids converting to Python tuples.

            Batches are imported zero-copy through the Arrow C Data
            Interface, so every Arrow type the JVM produces is supported,
            including ARRAY columns and decimal256.

            DECIMAL/NUMERIC columns beyond decimal256 capacity (declared
            precision > 76, or unbounded numerics) arrive as utf8 carrying
            jdbc_type/jdbc_precision/jdbc_scale field metadata, so consumers
            can recover the intended type and cast deliberately.
        """
        if not self._rs:
            raise Error("No result set")

        import pyarrow as pa
        it = self._get_iter()

        try:
            while it.hasNext():
                root = it.next()
                try:
                    # Zero-copy C Data Interface import. Unlike the
                    # pa.jvm bridge (primitive vectors only), this
                    # supports every Arrow type the Java side produces —
                    # including ARRAY (ListVector) and decimal256 columns
                    # (issue #125).
                    batch = _import_batch_via_cdata(root)
                    fallback = _decimal_fallback_indexes(
                        batch.schema, self._jdbc_decimal_info())
                    if fallback:
                        fields = []
                        for i, field in enumerate(batch.schema):
                            if i in fallback:
                                jdbc_type, precision, scale = fallback[i]
                                field = field.with_metadata(_decimal_field_metadata(
                                    jdbc_type, precision, scale))
                            fields.append(field)
                        batch = pa.RecordBatch.from_arrays(
                            batch.columns, schema=pa.schema(fields))
                    yield batch
                finally:
                    root.clear()
        finally:
            # Close iterator to release Arrow allocator and JDBC resources
            self._iter = None
            try:
                it.close()
            except Exception:
                pass

    def fetch_arrow_table(self):
        """
        Fetch all results as a single pyarrow.Table.

        This is a convenience method that collects all RecordBatches into one Table.

        Example:
            table = cursor.fetch_arrow_table()
            df = table.to_pandas()  # Efficient conversion to pandas

        Returns:
            pyarrow.Table: Complete result set as an Arrow Table
        """
        import pyarrow as pa
        batches = list(self.fetch_arrow_batches())
        if not batches:
            # Return empty table with inferred schema
            return pa.Table.from_arrays([])
        table = pa.Table.from_batches(batches)
        # Cast fallback columns back to decimal256 when the data fits.
        fallback = _decimal_fallback_indexes(
            table.schema, self._jdbc_decimal_info())
        for index in fallback:
            old_field = table.schema.field(index)
            column = table.column(index)
            if pa.types.is_list(old_field.type):
                # DECIMAL ARRAY fallback: scan the string elements, then
                # cast the child utf8 -> decimal256 when they all fit.
                elements = (value
                            for chunk in column.chunks
                            for row in chunk.to_pylist() if row is not None
                            for value in row if value is not None)
            else:
                elements = (value for chunk in column.chunks
                            for value in chunk.to_pylist())
            max_digits, max_scale = _infer_decimal_widths(elements)
            if max_digits > 76:
                continue
            if pa.types.is_list(old_field.type):
                target = pa.list_(pa.decimal256(max(max_digits, 1), max_scale))
            else:
                target = pa.decimal256(max(max_digits, 1), max_scale)
            try:
                casted = column.cast(target)
            except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
                continue
            jdbc_type, precision, scale = fallback[index]
            field = pa.field(
                old_field.name, target, nullable=old_field.nullable,
                metadata=_decimal_field_metadata(jdbc_type, precision, scale))
            table = table.set_column(index, field, casted)
        return table

    def fetch_df(self):
        """
        Fetch all results as a pandas DataFrame (optimized Arrow path).

        This is more efficient than fetchall() + manual pandas conversion
        because it uses Arrow's optimized pandas conversion.

        Example:
            df = cursor.fetch_df()
            # Work with DataFrame directly

        Returns:
            pandas.DataFrame: Query result as a pandas DataFrame
        """
        try:
            import pandas  # noqa: F401
        except ImportError:
            raise ImportError(
                "fetch_df() requires pandas. "
                "Install it with: pip install jaydebeapiarrow[pandas]"
            ) from None
        table = self.fetch_arrow_table()
        df = table.to_pandas()
        # Columns still utf8 (beyond decimal256) become Decimal objects too,
        # so every decimal column has the same object dtype.
        fallback = _decimal_fallback_indexes(
            table.schema, self._jdbc_decimal_info())
        for index in fallback:
            # Positional access: column names may repeat. isetitem replaces
            # the column wholesale (needed for the modern str dtype); pandas
            # < 1.5 falls back to iloc assignment, where string columns are
            # still object dtype. Decimal ARRAY columns arrive as lists of
            # strings; rebuild each element.
            def to_decimal(v):
                if isinstance(v, str):
                    return Decimal(v)
                if hasattr(v, "tolist"):
                    # to_pandas() materializes list cells as ndarrays
                    v = v.tolist()
                if isinstance(v, list):
                    return [Decimal(e) if isinstance(e, str) else e
                            for e in v]
                return v

            column = df.iloc[:, index].map(to_decimal)
            if hasattr(df, 'isetitem'):
                df.isetitem(index, column)
            else:
                df.iloc[:, index] = column
        return df

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

