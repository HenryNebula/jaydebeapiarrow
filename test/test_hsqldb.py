#-*- coding: utf-8 -*-

import jaydebeapiarrow
import os
import time
import unittest
from decimal import Decimal

try:
    import jaydebeapi
except ImportError:
    jaydebeapi = None

try:
    from test._base import IntegrationTestBase, _THIS_DIR, _SUPPRESS_LOGGING_ARGS
except ImportError:
    from _base import IntegrationTestBase, _THIS_DIR, _SUPPRESS_LOGGING_ARGS


class HsqldbTest(IntegrationTestBase, unittest.TestCase):

    def connect(self):
        # http://hsqldb.org/
        # hsqldb.jar
        driver, url, driver_args = ( 'org.hsqldb.jdbcDriver',
                                     'jdbc:hsqldb:mem:.',
                                     ['SA', ''] )
        return jaydebeapiarrow, self._quiet_connect(
            driver, url, driver_args)

    def setUpSql(self):
        self.sql_file(os.path.join(_THIS_DIR, 'data', 'create_hsqldb.sql'))
        self.sql_file(os.path.join(_THIS_DIR, 'data', 'insert.sql'))


class HsqldbMultipleConnectionsTest(unittest.TestCase):
    """Test that multiple sequential and simultaneous connections work (issue #97)."""

    def _connect(self, db_name):
        driver = 'org.hsqldb.jdbcDriver'
        url = f'jdbc:hsqldb:mem:{db_name}'
        return jaydebeapiarrow.connect(driver, url, ['SA', ''],
                                       jvm_args=_SUPPRESS_LOGGING_ARGS)

    def test_sequential_connections(self):
        """Connect, query, close, then connect again — each cycle should succeed."""
        for i in range(3):
            conn = self._connect(f'seq_test_{i}')
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1 FROM (VALUES(0))")
                rows = cursor.fetchall()
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0][0], 1)
            conn.close()

    def test_multiple_simultaneous_connections(self):
        """Multiple open connections at the same time should work independently."""
        connections = []
        for i in range(3):
            conn = self._connect(f'sim_test_{i}')
            connections.append(conn)

        for conn in connections:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1 FROM (VALUES(0))")
                rows = cursor.fetchall()
                self.assertEqual(len(rows), 1)

        for conn in connections:
            conn.close()


class HsqldbHighPrecisionNumericTest(unittest.TestCase):
    """High-precision NUMERIC columns must not crash the Arrow fetch path (issue #119)."""

    def setUp(self):
        self.conn = jaydebeapiarrow.connect(
            'org.hsqldb.jdbcDriver', 'jdbc:hsqldb:mem:hpnumeric',
            ['SA', ''],
            jvm_args=_SUPPRESS_LOGGING_ARGS)

    def tearDown(self):
        self.conn.close()

    def test_numeric_precision_50_scale_30(self):
        """NUMERIC(50, 30) — decimal256 range. Supported by upstream
        arrow-jdbc, but crashed with 'Decimal size greater than 16 bytes'
        under our decimal128-only type mapping."""
        with self.conn.cursor() as cursor:
            cursor.execute("CREATE TABLE t_hp1 (val NUMERIC(50, 30))")
            cursor.execute(
                "INSERT INTO t_hp1 VALUES "
                "(12345678901234567890.123456789012345678901234567890)")
            cursor.execute("SELECT val FROM t_hp1")
            result = cursor.fetchone()
        self.assertEqual(
            result[0], Decimal("12345678901234567890.123456789012345678901234567890"))

    def test_numeric_1000_64_issue_119(self):
        """Exact scenario from issue #119: NUMERIC(1000, 64) crashed with
        'Decimal size greater than 16 bytes: 28'."""
        with self.conn.cursor() as cursor:
            cursor.execute("CREATE TABLE t_hp2 (val NUMERIC(1000, 64))")
            cursor.execute("INSERT INTO t_hp2 VALUES (123.4567)")
            cursor.execute("SELECT val FROM t_hp2")
            result = cursor.fetchone()
        self.assertEqual(result[0], Decimal("123.4567"))

    def test_numeric_1000_80_digit_value(self):
        """An 80-digit value in NUMERIC(1000, 0) exceeds decimal256's 76-digit
        capacity; it should still round-trip exactly via the string path."""
        literal = "1" + "0" * 79
        with self.conn.cursor() as cursor:
            cursor.execute("CREATE TABLE t_hp3 (val NUMERIC(1000, 0))")
            cursor.execute(f"INSERT INTO t_hp3 VALUES ({literal})")
            cursor.execute("SELECT val FROM t_hp3")
            result = cursor.fetchone()
        self.assertIsInstance(result[0], Decimal)
        self.assertEqual(result[0], Decimal(literal))

    def test_arrow_paths_high_precision_numeric(self):
        """Arrow-native paths keep decimal semantics where possible:
        batches label fallback columns with jdbc metadata, tables cast them
        back to decimal256 when the data fits (issue #119)."""
        import pyarrow as pa
        with self.conn.cursor() as cursor:
            cursor.execute("CREATE TABLE t_hp4 (val NUMERIC(1000, 64))")
            cursor.execute("INSERT INTO t_hp4 VALUES (123.4567)")

            cursor.execute("SELECT val FROM t_hp4")
            batch = next(cursor.fetch_arrow_batches())
            self.assertTrue(pa.types.is_string(batch.schema.field(0).type))
            self.assertEqual(
                batch.schema.field(0).metadata.get(b"jdbc_precision"), b"1000")
            self.assertEqual(
                batch.schema.field(0).metadata.get(b"jdbc_scale"), b"64")

            cursor.execute("SELECT val FROM t_hp4")
            table = cursor.fetch_arrow_table()
            self.assertTrue(
                pa.types.is_decimal256(table.schema.field(0).type))
            self.assertEqual(
                table.column(0).to_pylist()[0], Decimal("123.4567"))


class HsqldbArrayTypeTest(unittest.TestCase):
    """Test ARRAY type support — reading and writing with multiple element types."""

    def setUp(self):
        self.conn = jaydebeapiarrow.connect(
            'org.hsqldb.jdbcDriver', 'jdbc:hsqldb:mem:arraytest',
            ['SA', ''],
            jvm_args=_SUPPRESS_LOGGING_ARGS)
        with self.conn.cursor() as cursor:
            cursor.execute(
                "CREATE TABLE test_arrays ("
                "  id INT, "
                "  int_vals INT ARRAY, "
                "  str_vals VARCHAR(100) ARRAY, "
                "  bool_vals BOOLEAN ARRAY, "
                "  float_vals DOUBLE ARRAY)")
            cursor.execute(
                "INSERT INTO test_arrays VALUES ("
                "  1, "
                "  ARRAY[10, 20, 30], "
                "  ARRAY['foo', 'bar', 'baz'], "
                "  ARRAY[TRUE, FALSE, TRUE], "
                "  ARRAY[1.5, 2.5, 3.5])")

    def tearDown(self):
        with self.conn.cursor() as cursor:
            cursor.execute("DROP TABLE test_arrays IF EXISTS")
        self.conn.close()

    def test_read_integer_array(self):
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT int_vals FROM test_arrays WHERE id = 1")
            result = cursor.fetchone()
        self.assertIsInstance(result[0], list)
        self.assertEqual(result[0], [10, 20, 30])

    def test_read_varchar_array(self):
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT str_vals FROM test_arrays WHERE id = 1")
            result = cursor.fetchone()
        self.assertIsInstance(result[0], list)
        self.assertEqual(result[0], ["foo", "bar", "baz"])

    def test_read_boolean_array(self):
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT bool_vals FROM test_arrays WHERE id = 1")
            result = cursor.fetchone()
        self.assertIsInstance(result[0], list)
        self.assertEqual(result[0], [True, False, True])

    def test_read_float_array(self):
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT float_vals FROM test_arrays WHERE id = 1")
            result = cursor.fetchone()
        self.assertIsInstance(result[0], list)
        self.assertEqual(result[0], [1.5, 2.5, 3.5])

    def test_bind_string_list(self):
        with self.conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO test_arrays (id, str_vals) VALUES (?, ?)",
                (2, ["one", "two"]))
            cursor.execute("SELECT str_vals FROM test_arrays WHERE id = 2")
            result = cursor.fetchone()
        self.assertIsInstance(result[0], list)
        self.assertEqual(result[0], ["one", "two"])

    def test_bind_int_list(self):
        with self.conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO test_arrays (id, int_vals) VALUES (?, ?)",
                (3, [100, 200]))
            cursor.execute("SELECT int_vals FROM test_arrays WHERE id = 3")
            result = cursor.fetchone()
        self.assertIsInstance(result[0], list)
        self.assertEqual(result[0], [100, 200])

    def test_read_multiple_array_columns(self):
        """Multiple ARRAY columns in a single row."""
        with self.conn.cursor() as cursor:
            cursor.execute(
                "SELECT int_vals, str_vals FROM test_arrays WHERE id = 1")
            result = cursor.fetchone()
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], [10, 20, 30])
        self.assertEqual(result[1], ["foo", "bar", "baz"])


class HsqldbFetchPerformanceTest(unittest.TestCase):
    """Relative speed vs the row-by-row JPype path (original jaydebeapi),
    on small in-memory tables: only the ratio is asserted, never an
    absolute duration, so the result is machine-independent. Required
    speedups sit far below the measured ones (~5.5-7x mixed columns,
    ~3-4x high-precision fallback) so CI jitter cannot flip them.
    """

    ROWS = 10000
    REPS = 3
    REQUIRED_SPEEDUP = 2.0
    REQUIRED_SPEEDUP_HIGH_PRECISION = 1.5

    HP_VALUE = ('123.4567890123456789012345678901234567890'
                '123456789012345678901234')

    @classmethod
    def setUpClass(cls):
        if jaydebeapi is None:
            raise unittest.SkipTest(
                'original jaydebeapi is not installed; install it to run '
                'the performance regression tests')
        cls.conn = jaydebeapiarrow.connect(
            'org.hsqldb.jdbcDriver', 'jdbc:hsqldb:mem:perftest',
            ['SA', ''],
            jvm_args=_SUPPRESS_LOGGING_ARGS)
        # Same in-memory DB through the row-by-row library, reusing the
        # running JVM.
        cls.legacy_conn = jaydebeapi.connect(
            'org.hsqldb.jdbcDriver', 'jdbc:hsqldb:mem:perftest', ['SA', ''])
        with cls.conn.cursor() as cursor:
            cursor.execute(
                'CREATE TABLE perf_mixed ('
                'id BIGINT, int_val INT, double_val DOUBLE, '
                'str_val VARCHAR(64), dec_val NUMERIC(20, 4))')
            cursor.execute(
                'INSERT INTO perf_mixed VALUES ('
                "1, 42, 3.14, 'the quick brown fox jumps', 12345.6789)")
            cls._double_rows(cursor, 'perf_mixed',
                             ('id', 'int_val', 'double_val',
                              'str_val', 'dec_val'), cls.ROWS)
            cursor.execute('CREATE TABLE perf_hp (id BIGINT, val NUMERIC(1000, 64))')
            cursor.execute('INSERT INTO perf_hp VALUES (1, %s)' % cls.HP_VALUE)
            cls._double_rows(cursor, 'perf_hp', ('id', 'val'), cls.ROWS)

    @classmethod
    def tearDownClass(cls):
        with cls.conn.cursor() as cursor:
            cursor.execute('DROP TABLE perf_mixed IF EXISTS')
            cursor.execute('DROP TABLE perf_hp IF EXISTS')
        cls.conn.close()
        cls.legacy_conn.close()

    @staticmethod
    def _double_rows(cursor, table, columns, target):
        """Grow `table` to `target` rows by repeated INSERT..SELECT doubling."""
        count = 1
        while count < target:
            step = min(count, target - count)
            select_list = ', '.join(
                '%s + %d' % (col, count) if col == 'id' else col
                for col in columns)
            cursor.execute(
                'INSERT INTO %s SELECT %s FROM %s WHERE id <= %d'
                % (table, select_list, table, step))
            count += step

    def _fetchall(self, connection, table):
        with connection.cursor() as cursor:
            cursor.execute('SELECT * FROM %s' % table)
            return cursor.fetchall()

    @staticmethod
    def _best_of_ms(fetch, reps):
        """Fastest of `reps` timed calls, in milliseconds, plus the result."""
        best_ms = None
        rows = None
        for _ in range(reps):
            start = time.perf_counter()
            rows = fetch()
            elapsed = (time.perf_counter() - start) * 1000.0
            best_ms = elapsed if best_ms is None else min(best_ms, elapsed)
        return best_ms, rows

    def _assert_faster(self, table, required_speedup):
        """Assert fetchall() through Arrow stays ahead of jaydebeapi on
        identical data."""
        # Warmup: class loading and JIT otherwise dominate the first run.
        self._fetchall(self.conn, table)
        self._fetchall(self.legacy_conn, table)

        arrow_ms, arrow_rows = self._best_of_ms(
            lambda: self._fetchall(self.conn, table), self.REPS)
        legacy_ms, legacy_rows = self._best_of_ms(
            lambda: self._fetchall(self.legacy_conn, table), self.REPS)

        self.assertEqual(len(arrow_rows), self.ROWS)
        self.assertEqual(len(legacy_rows), self.ROWS)
        # Compare only columns where jaydebeapi returns Python natives;
        # its NUMERIC converter degrades scaled values to doubles.
        for arrow_row, legacy_row in zip(arrow_rows[:1] + arrow_rows[-1:],
                                         legacy_rows[:1] + legacy_rows[-1:]):
            self.assertEqual(arrow_row[0], legacy_row[0])

        self.assertGreaterEqual(
            legacy_ms / arrow_ms, required_speedup,
            'Arrow fetchall() (%.1f ms) lost its speedup over the JPype '
            'row-by-row path (%.1f ms)' % (arrow_ms, legacy_ms))
        return arrow_rows

    def test_fetchall_faster_than_jaydebeapi(self):
        """fetchall() must stay well ahead of the row-by-row path on
        typical mixed-column data."""
        arrow_rows = self._assert_faster('perf_mixed', self.REQUIRED_SPEEDUP)
        self.assertEqual(arrow_rows[0][3], 'the quick brown fox jumps')
        self.assertEqual(arrow_rows[0][4], Decimal('12345.6789'))

    def test_high_precision_decimal_fetch_faster_than_jaydebeapi(self):
        """The issue #119 string fallback must stay faster than the
        row-by-row path it replaces."""
        arrow_rows = self._assert_faster(
            'perf_hp', self.REQUIRED_SPEEDUP_HIGH_PRECISION)
        self.assertEqual(arrow_rows[0][1], Decimal(self.HP_VALUE))
