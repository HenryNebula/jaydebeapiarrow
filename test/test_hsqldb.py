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
    """Verify ARRAY type handling with HSQLDB (legacy issue baztian/jaydebeapi#159)."""

    def setUp(self):
        driver = 'org.hsqldb.jdbcDriver'
        url = 'jdbc:hsqldb:mem:array_test'
        self.conn = jaydebeapiarrow.connect(driver, url, ['SA', ''],
                                            experimental={'jvm_args': _SUPPRESS_LOGGING_ARGS})

    def tearDown(self):
        self.conn.close()

    def test_array_column_read(self):
        """Verify ARRAY columns are readable as strings via ExplicitTypeMapper
        VARCHAR fallback. Regression test for legacy issue baztian/jaydebeapi#159."""
        with self.conn.cursor() as cursor:
            cursor.execute("CREATE TABLE test_array_type (id INT, data INTEGER ARRAY)")
            try:
                cursor.execute(
                    "INSERT INTO test_array_type (id, data) VALUES (1, ARRAY[1,2,3])"
                )
                cursor.execute("SELECT data FROM test_array_type WHERE id = 1")
                result = cursor.fetchone()
                self.assertIsInstance(result[0], str)
                self.assertIs(cursor.description[0][1], jaydebeapiarrow.ARRAY)
            finally:
                cursor.execute("DROP TABLE IF EXISTS test_array_type")
