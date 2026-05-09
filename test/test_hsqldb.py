#-*- coding: utf-8 -*-

import jaydebeapiarrow
import os
import unittest

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
        return jaydebeapiarrow, jaydebeapiarrow.connect(
            driver, url, driver_args,
            experimental={'jvm_args': _SUPPRESS_LOGGING_ARGS})

    def setUpSql(self):
        self.sql_file(os.path.join(_THIS_DIR, 'data', 'create_hsqldb.sql'))
        self.sql_file(os.path.join(_THIS_DIR, 'data', 'insert.sql'))


class HsqldbMultipleConnectionsTest(unittest.TestCase):
    """Test that multiple sequential and simultaneous connections work (issue #97)."""

    def _connect(self, db_name):
        driver = 'org.hsqldb.jdbcDriver'
        url = f'jdbc:hsqldb:mem:{db_name}'
        return jaydebeapiarrow.connect(driver, url, ['SA', ''],
                                       experimental={'jvm_args': _SUPPRESS_LOGGING_ARGS})

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


class HsqldbArrayTypeTest(unittest.TestCase):
    """Test ARRAY type support — reading (VARCHAR fallback) and writing (list binding)."""

    def setUp(self):
        self.conn = jaydebeapiarrow.connect(
            'org.hsqldb.jdbcDriver', 'jdbc:hsqldb:mem:arraytest',
            ['SA', ''],
            experimental={'jvm_args': _SUPPRESS_LOGGING_ARGS})
        with self.conn.cursor() as cursor:
            cursor.execute(
                "CREATE TABLE test_arrays (id INT, tags VARCHAR(100) ARRAY)")
            cursor.execute(
                "INSERT INTO test_arrays VALUES (1, ARRAY['foo', 'bar', 'baz'])")

    def tearDown(self):
        with self.conn.cursor() as cursor:
            cursor.execute("DROP TABLE test_arrays IF EXISTS")
        self.conn.close()

    def test_array_column_read_as_varchar_string(self):
        """ARRAY column currently returns as VARCHAR string (pyarrow.jvm List limitation)."""
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT tags FROM test_arrays WHERE id = 1")
            result = cursor.fetchone()
        self.assertIsInstance(result[0], str)

    def test_array_parameter_binding(self):
        """Python list parameters should be bindable as SQL ARRAYs."""
        with self.conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO test_arrays (id, tags) VALUES (?, ?)",
                (4, ["one", "two"]))
            cursor.execute("SELECT tags FROM test_arrays WHERE id = 4")
            result = cursor.fetchone()
        self.assertIsInstance(result[0], str)
