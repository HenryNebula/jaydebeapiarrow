#-*- coding: utf-8 -*-

import jaydebeapiarrow
import os
import unittest
from decimal import Decimal

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
