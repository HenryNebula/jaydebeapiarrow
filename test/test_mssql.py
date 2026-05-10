#-*- coding: utf-8 -*-

import jaydebeapiarrow
import os
import unittest

try:
    from test._base import IntegrationTestBase, _THIS_DIR
except ImportError:
    from _base import IntegrationTestBase, _THIS_DIR


class MSSQLTest(IntegrationTestBase, unittest.TestCase):

    def connect(self):

        import jpype

        host = os.environ.get("JY_MSSQL_HOST", "localhost")
        port = os.environ.get("JY_MSSQL_PORT", "11433")
        user = os.environ.get("JY_MSSQL_USER", "sa")
        password = os.environ.get("JY_MSSQL_PASSWORD", "Password123!")

        driver, url, driver_args = (
            'com.microsoft.sqlserver.jdbc.SQLServerDriver',
            f'jdbc:sqlserver://{host}:{port};encrypt=false;trustServerCertificate=true',
            {'user': user, 'password': password}
        )

        try:
            db, conn = jaydebeapiarrow, self._quiet_connect(
                driver, url, driver_args)
        except jpype.JException:
            self.fail("Can not connect with MS SQL Server. Please check if the instance is up and running.")
        else:
            return db, conn

    def setUpSql(self):
        with self.conn.cursor() as cursor:
            cursor.execute("IF DB_ID('test_db') IS NULL CREATE DATABASE test_db")
            cursor.execute("USE test_db")
        self.sql_file(os.path.join(_THIS_DIR, 'data', 'create_mssql.sql'))
        self.sql_file(os.path.join(_THIS_DIR, 'data', 'insert.sql'))

    def _cleanup_tables(self):
        with self.conn.cursor() as cursor:
            try:
                cursor.execute("USE test_db")
            except Exception:
                pass
        super()._cleanup_tables()

    def tearDown(self):
        with self.conn.cursor() as cursor:
            cursor.execute("USE test_db")
        super().tearDown()

    def _double_create_sql(self):
        return "CREATE TABLE DOUBLE_TEST (val FLOAT)"

    def _autoincrement_create_sql(self):
        return ("CREATE TABLE LASTROWID_TEST "
                "(id INT IDENTITY(1,1) PRIMARY KEY, "
                "val VARCHAR(50))")

    def test_blob_null_value(self):
        """MSSQL JDBC driver rejects NULL parameter binding for VARBINARY columns."""
        self.skipTest("MSSQL JDBC driver does not support NULL for VARBINARY parameter binding")
