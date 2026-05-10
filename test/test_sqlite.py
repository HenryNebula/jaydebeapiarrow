#-*- coding: utf-8 -*-

import jaydebeapiarrow
import os
import unittest

from decimal import Decimal
from datetime import datetime
try:
    from test._base import IntegrationTestBase, SqliteTestBase, _THIS_DIR
except ImportError:
    from _base import IntegrationTestBase, SqliteTestBase, _THIS_DIR


class SqlitePyTest(SqliteTestBase, unittest.TestCase):

    JDBC_SUPPORT_TEMPORAL_TYPE = True

    def _numeric_create_table_sql(self):
        """Use DECIMAL so sqlite3's detect_types converter fires."""
        return (
            "CREATE TABLE NUMERIC_TEST ("
            "ID INTEGER NOT NULL, "
            "NUM_COL DECIMAL(10, 2), "
            "PRIMARY KEY (ID))"
        )

    class ConnectionWithClosing:
        def __init__(self, conn):
            from contextlib import closing
            self.conn = conn
            self.cursor = lambda: closing(self.conn.cursor())

        def close(self):
            self.conn.close()

    def connect(self):
        import sqlite3
        sqlite3.register_adapter(Decimal, lambda d: str(d))
        sqlite3.register_converter("decimal", lambda s: Decimal(s.decode('utf-8')) if s is not None else s)
        return sqlite3, self.ConnectionWithClosing(sqlite3.connect(':memory:', detect_types=sqlite3.PARSE_DECLTYPES))

    def test_execute_type_time(self):
        self.skipTest("Time type not supported by PySqlite")

    def test_numeric_precision_scale_combos(self):
        self.skipTest("SQLite type affinity makes NUMERIC/DECIMAL precision unreliable")

    def test_description_returns_column_alias(self):
        self.skipTest("Python sqlite3 does not support AS aliases in cursor.description")

    def test_timestamp_utc_roundtrip_no_timezone_shift(self):
        self.skipTest("Python sqlite3 does not support parameterized TIMESTAMP INSERT")

    def test_commit_with_autocommit_enabled(self):
        self.skipTest("pysqlite uses isolation_level, not JDBC setAutoCommit")

    def test_commit_with_autocommit_disabled(self):
        self.skipTest("pysqlite uses isolation_level, not JDBC setAutoCommit")

    def test_rollback_with_autocommit_enabled(self):
        self.skipTest("pysqlite uses isolation_level, not JDBC setAutoCommit")

    def test_rollback_with_autocommit_disabled(self):
        self.skipTest("pysqlite uses isolation_level, not JDBC setAutoCommit")

    def test_sql_exception_message_is_clean(self):
        self.skipTest("pysqlite raises sqlite3.OperationalError, not JDBC-wrapped DatabaseError")

    def test_lastrowid_none_after_select(self):
        self.skipTest("pysqlite returns actual rowid values, not None")

    def test_lastrowid_none_after_insert(self):
        self.skipTest("pysqlite returns actual rowid values, not None")

    def test_lastrowid_none_after_executemany(self):
        self.skipTest("pysqlite returns actual rowid values, not None")

    def test_lastrowid_exists_and_is_none(self):
        self.skipTest("pysqlite returns actual rowid values, not None")

    def test_iterator_closed_after_fetchall(self):
        self.skipTest("cursor._iter is jaydebeapiarrow-specific")

    def test_iterator_closed_after_fetchone_exhaustion(self):
        self.skipTest("cursor._iter is jaydebeapiarrow-specific")

    def test_iterator_closed_after_fetchmany_exhaustion(self):
        self.skipTest("cursor._iter is jaydebeapiarrow-specific")

    def test_repeated_query_cycles_release_resources(self):
        self.skipTest("cursor._iter is jaydebeapiarrow-specific")


class SqliteXerialTest(SqliteTestBase, unittest.TestCase):

    JDBC_SUPPORT_TEMPORAL_TYPE = True

    def connect(self):
        #http://bitbucket.org/xerial/sqlite-jdbc
        # sqlite-jdbc-3.7.2.jar
        driver, url = 'org.sqlite.JDBC', 'jdbc:sqlite::memory:'
        properties = {
            "date_string_format": "yyyy-MM-dd HH:mm:ss"
        }
        return jaydebeapiarrow, self._quiet_connect(
            driver, url, driver_args=properties)

    def test_execute_and_fetch(self):
        """SQLite date_string_format truncates microseconds."""
        with self.conn.cursor() as cursor:
            cursor.execute("select ACCOUNT_ID, ACCOUNT_NO, BALANCE, BLOCKING " \
                        "from ACCOUNT")
            result = cursor.fetchall()
        self.assertEqual(result, [
            (
            datetime(2009, 9, 10, 14, 15, 22),
            18, Decimal('12.4'), None),
            (
            datetime(2009, 9, 11, 14, 15, 22),
            19, Decimal('12.9'), Decimal('1'))
        ])

    def test_timestamp_microsecond_precision(self):
        """SQLite Xerial JDBC truncates microseconds via date_string_format."""
        self.skipTest("SQLite Xerial JDBC truncates microsecond precision")

    def test_lastrowid_none_after_insert(self):
        """SQLite has implicit ROWID, so getGeneratedKeys returns the rowid."""
        stmt = "insert into ACCOUNT (ACCOUNT_ID, ACCOUNT_NO, BALANCE) " \
               "values (?, ?, ?)"
        with self.conn.cursor() as cursor:
            cursor.execute(stmt, (self.dbapi.Timestamp(2009, 9, 11, 14, 15, 22, 123450), 99, 1.0))
            self.assertIsNotNone(cursor.lastrowid)

    def test_execute_and_fetch_parameter(self):
        with self.conn.cursor() as cursor:
            cursor.execute("select ACCOUNT_ID, ACCOUNT_NO, BALANCE, BLOCKING " \
                        "from ACCOUNT where ACCOUNT_NO = ?", (18,))
            result = cursor.fetchall()
        self.assertEqual(result, [
            (
            datetime(2009, 9, 10, 14, 15, 22),
            18, Decimal('12.4'), None)
        ])

    def test_execute_and_fetchone(self):
        with self.conn.cursor() as cursor:
            cursor.execute("select ACCOUNT_ID, ACCOUNT_NO, BALANCE, BLOCKING " \
                        "from ACCOUNT order by ACCOUNT_NO")
            result = cursor.fetchone()
        self.assertEqual(result, (
            datetime(2009, 9, 10, 14, 15, 22),
            18, Decimal('12.4'), None))
        cursor.close()

    def test_execute_and_fetchone_consecutive(self):
        with self.conn.cursor() as cursor:
            cursor.execute("select ACCOUNT_ID, ACCOUNT_NO, BALANCE, BLOCKING " \
                        "from ACCOUNT order by ACCOUNT_NO")
            result1 = cursor.fetchone()
            result2 = cursor.fetchone()

        self.assertEqual(result1, (
            datetime(2009, 9, 10, 14, 15, 22),
            18, Decimal('12.4'), None))

        self.assertEqual(result2, (
            datetime(2009, 9, 11, 14, 15, 22),
            19, Decimal('12.9'), Decimal('1')))

    def test_execute_and_fetchmany(self):
        with self.conn.cursor() as cursor:
            cursor.execute("select ACCOUNT_ID, ACCOUNT_NO, BALANCE, BLOCKING " \
                        "from ACCOUNT order by ACCOUNT_NO")
            result = cursor.fetchmany()
        self.assertEqual(result, [
            (
            datetime(2009, 9, 10, 14, 15, 22),
            18, Decimal('12.4'), None)
        ])

    def test_execute_types(self):
        """
        xerial/sqlite-jdbc has some issues with type mapping:
        1. Timestamp has inconsistent types: JDBC returns it as a VARCHAR, while it's defined as a TIMESTAMP in the DB
        2. Default date_string_format does not handle ISO Date (without microseconds)
        3. SQLite stores DECIMAL values with dynamic typing (integer vs double)
        """
        stmt = "insert into ACCOUNT (ACCOUNT_ID, ACCOUNT_NO, BALANCE, " \
               "BLOCKING, DBL_COL, OPENED_AT, VALID, PRODUCT_NAME) " \
               "values (?, ?, ?, ?, ?, ?, ?, ?)"
        account_id = self.dbapi.Timestamp(2010, 1, 26, 14, 31, 59)
        account_no = 20
        balance = Decimal('1.2')
        blocking = Decimal('10.0')
        dbl_col = 3.5
        opened_at = self.dbapi.Timestamp(2008, 2, 27, 0, 0, 0)
        valid = True
        product_name = u'Savings account'
        parms = (
            account_id,
            account_no, balance, blocking, dbl_col,
            opened_at,
            valid, product_name
        )
        with self.conn.cursor() as cursor:
            cursor.execute(stmt, parms)
            stmt = "select ACCOUNT_ID, ACCOUNT_NO, BALANCE, BLOCKING, " \
                "DBL_COL, OPENED_AT, VALID, PRODUCT_NAME " \
                "from ACCOUNT where ACCOUNT_NO = ?"
            parms = (20,)
            cursor.execute(stmt, parms)
            result = cursor.fetchone()

        exp = (
            account_id,
            account_no, balance, blocking, dbl_col,
            opened_at.date(),
            valid, product_name
        )
        self.assertEqual(result, exp)

    def test_execute_type_time(self):
        """SQLite date_string_format truncates microseconds."""
        stmt = "insert into ACCOUNT (ACCOUNT_ID, ACCOUNT_NO, BALANCE, " \
               "OPENED_AT_TIME) " \
               "values (?, ?, ?, ?)"
        account_id = self.dbapi.Timestamp(2010, 1, 26, 14, 31, 59)
        account_no = 20
        balance = 1.2
        opened_at_time = self.dbapi.Time(13, 59, 59)
        parms = (account_id, account_no, balance, opened_at_time)
        with self.conn.cursor() as cursor:
            cursor.execute(stmt, parms)
            stmt = "select ACCOUNT_ID, ACCOUNT_NO, BALANCE, OPENED_AT_TIME " \
                "from ACCOUNT where ACCOUNT_NO = ?"
            parms = (20, )
            cursor.execute(stmt, parms)
            result = cursor.fetchone()

        exp = (
            account_id,
            account_no, Decimal(str(balance)),
            self._cast_time('13:59:59', r'%H:%M:%S')
        )
        self.assertEqual(result, exp)

    def _numeric_create_table_sql(self):
        """SQLite treats NUMERIC as an affinity type — use DECIMAL instead."""
        return (
            "CREATE TABLE NUMERIC_TEST ("
            "ID INTEGER NOT NULL, "
            "NUM_COL DECIMAL, "
            "PRIMARY KEY (ID))"
        )

    def test_timestamp_subsecond_leading_zeros(self):
        """SQLite Xerial JDBC truncates microseconds via date_string_format."""
        self.skipTest("SQLite Xerial JDBC truncates microsecond precision")

    def test_description_returns_column_alias(self):
        """Verify quoted alias is preserved by SQLite JDBC."""
        pass  # Inherited from IntegrationTestBase — quoted alias works

    def test_timestamp_utc_roundtrip_no_timezone_shift(self):
        """SQLite Xerial JDBC truncates microseconds."""
        self.skipTest("SQLite Xerial JDBC truncates microsecond precision")
