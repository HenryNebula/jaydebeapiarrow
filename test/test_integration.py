#-*- coding: utf-8 -*-

# Copyright 2010 Bastian Bowe
#
# This file is part of JayDeBeApi.
# JayDeBeApi is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
# 
# JayDeBeApi is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
# 
# You should have received a copy of the GNU Lesser General Public
# License along with JayDeBeApi.  If not, see
# <http://www.gnu.org/licenses/>.
#
# Modified by HenryNebula:
# 1. Remove py2 & Jython support
# 2. Modify test to enforce typing for Decimal and temporal types


import jaydebeapiarrow

import os
import sys
import threading

import unittest

from decimal import Decimal
from datetime import datetime, timezone
from collections import namedtuple

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))


class IntegrationTestBase(object):

    JDBC_SUPPORT_TEMPORAL_TYPE = True

    def _cast_datetime(self, datetime_str, fmt=r'%Y-%m-%d %H:%M:%S'):
        if self.JDBC_SUPPORT_TEMPORAL_TYPE and type(datetime_str) == str:
            return datetime.strptime(datetime_str, fmt)
        else:
            return datetime_str

    def _cast_time(self, time_str, fmt=r'%H:%M:%S'):
        if self.JDBC_SUPPORT_TEMPORAL_TYPE and type(time_str) == str:
            return datetime.strptime(time_str, fmt).time()
        else:
            return time_str

    def _cast_date(self, date_str, fmt=r'%Y-%m-%d'):
        if self.JDBC_SUPPORT_TEMPORAL_TYPE and type(date_str) == str:
            return datetime.strptime(date_str, fmt).date()
        else:
            return date_str

    def sql_file(self, filename):
        f = open(filename, 'r')
        try:
            lines = f.readlines()
        finally:
            f.close()
        stmt = []
        stmts = []
        for i in lines:
            stmt.append(i)
            if ";" in i:
                stmts.append(" ".join(stmt))
                stmt = []
        with self.conn.cursor() as cursor:
            for i in stmts:
                cursor.execute(i)

    def setUp(self):
        (self.dbapi, self.conn) = self.connect()
        self.setUpSql()

    def setUpSql(self):
        raise NotImplementedError

    def connect(self):
        raise NotImplementedError

    def tearDown(self):
        with self.conn.cursor() as cursor:
            cursor.execute("drop table ACCOUNT")
        self.conn.close()

    def test_execute_and_fetch_no_data(self):
        with self.conn.cursor() as cursor:
            stmt = "select * from ACCOUNT where ACCOUNT_ID is null"
            cursor.execute(stmt)
            result = cursor.fetchall()
        self.assertEqual(result, [])

    def test_execute_and_fetch(self):
        with self.conn.cursor() as cursor:
            cursor.execute("select ACCOUNT_ID, ACCOUNT_NO, BALANCE, BLOCKING " \
                        "from ACCOUNT")
            result = cursor.fetchall()
        self.assertEqual(result, [
            (
            self._cast_datetime('2009-09-10 14:15:22.123456', r'%Y-%m-%d %H:%M:%S.%f'),
            18, Decimal('12.4'), None),
            (
            self._cast_datetime('2009-09-11 14:15:22.123456', r'%Y-%m-%d %H:%M:%S.%f'),
            19, Decimal('12.9'), Decimal('1'))
        ])

    def test_execute_and_fetch_parameter(self):
        with self.conn.cursor() as cursor:
            cursor.execute("select ACCOUNT_ID, ACCOUNT_NO, BALANCE, BLOCKING " \
                        "from ACCOUNT where ACCOUNT_NO = ?", (18,))
            result = cursor.fetchall()
        self.assertEqual(result, [
            (
            self._cast_datetime('2009-09-10 14:15:22.123456', r'%Y-%m-%d %H:%M:%S.%f'),
            18, Decimal('12.4'), None)
        ])

    def test_execute_and_fetchone(self):
        with self.conn.cursor() as cursor:
            cursor.execute("select ACCOUNT_ID, ACCOUNT_NO, BALANCE, BLOCKING " \
                        "from ACCOUNT order by ACCOUNT_NO")
            result = cursor.fetchone()
        self.assertEqual(result, (
            self._cast_datetime('2009-09-10 14:15:22.123456', r'%Y-%m-%d %H:%M:%S.%f'),
            18, Decimal('12.4'), None))
        cursor.close()

    def test_execute_reset_description_without_execute_result(self):
        """Expect the descriptions property being reset when no query
        has been made via execute method.
        """
        with self.conn.cursor() as cursor:
            cursor.execute("select * from ACCOUNT")
            self.assertIsNotNone(cursor.description)
            cursor.fetchone()
            cursor.execute("delete from ACCOUNT")
            self.assertIsNone(cursor.description)

    def test_execute_and_fetchone_after_end(self):
        with self.conn.cursor() as cursor:
            cursor.execute("select * from ACCOUNT where ACCOUNT_NO = ?", (18,))
            cursor.fetchone()
            result = cursor.fetchone()
        self.assertIsNone(result)

    def test_execute_and_fetchone_consecutive(self):
        with self.conn.cursor() as cursor:
            cursor.execute("select ACCOUNT_ID, ACCOUNT_NO, BALANCE, BLOCKING " \
                        "from ACCOUNT order by ACCOUNT_NO")
            result1 = cursor.fetchone()
            result2 = cursor.fetchone()
        
        self.assertEqual(result1, (
            self._cast_datetime('2009-09-10 14:15:22.123456', r'%Y-%m-%d %H:%M:%S.%f'),
            18, Decimal('12.4'), None))
        
        self.assertEqual(result2, (
            self._cast_datetime('2009-09-11 14:15:22.123456', r'%Y-%m-%d %H:%M:%S.%f'),
            19, Decimal('12.9'), Decimal('1')))

    def test_execute_and_fetchmany(self):
        with self.conn.cursor() as cursor:
            cursor.execute("select ACCOUNT_ID, ACCOUNT_NO, BALANCE, BLOCKING " \
                        "from ACCOUNT order by ACCOUNT_NO")
            result = cursor.fetchmany()
        self.assertEqual(result, [
            (
            self._cast_datetime('2009-09-10 14:15:22.123456', r'%Y-%m-%d %H:%M:%S.%f'),
            18, Decimal('12.4'), None)
        ])
        # TODO: find out why this cursor has to be closed in order to
        # let this test work with sqlite if __del__ is not overridden
        # in cursor
        # cursor.close()

    def test_executemany(self):
        stmt = "insert into ACCOUNT (ACCOUNT_ID, ACCOUNT_NO, BALANCE) " \
               "values (?, ?, ?)"
        parms = (
            ( self.dbapi.Timestamp(2009, 9, 11, 14, 15, 22, 123450), 20, 13.1 ),
            ( self.dbapi.Timestamp(2009, 9, 11, 14, 15, 22, 123451), 21, 13.2 ),
            ( self.dbapi.Timestamp(2009, 9, 11, 14, 15, 22, 123452), 22, 13.3 ),
            )
        with self.conn.cursor() as cursor:
            cursor.executemany(stmt, parms)
            self.assertEqual(cursor.rowcount, 3)

    def test_execute_types(self):
        stmt = "insert into ACCOUNT (ACCOUNT_ID, ACCOUNT_NO, BALANCE, " \
               "BLOCKING, DBL_COL, OPENED_AT, VALID, PRODUCT_NAME) " \
               "values (?, ?, ?, ?, ?, ?, ?, ?)"
        account_id = self.dbapi.Timestamp(2010, 1, 26, 14, 31, 59)
        account_no = 20
        balance = Decimal('1.2')
        blocking = 10.0
        dbl_col = 3.5
        opened_at = self.dbapi.Date(1908, 2, 27)
        valid = True
        product_name = u'Savings account'
        parms = (account_id, account_no, balance, blocking, dbl_col,
                 opened_at, valid, product_name)
        with self.conn.cursor() as cursor:
            cursor.execute(stmt, parms)
            stmt = "select ACCOUNT_ID, ACCOUNT_NO, BALANCE, BLOCKING, " \
                "DBL_COL, OPENED_AT, VALID, PRODUCT_NAME " \
                "from ACCOUNT where ACCOUNT_NO = ?"
            parms = (20, )
            cursor.execute(stmt, parms)
            result = cursor.fetchone()
        exp = (
            self._cast_datetime('2010-01-26 14:31:59', r'%Y-%m-%d %H:%M:%S'),
            account_no, balance, blocking, dbl_col,
            self._cast_date('1908-02-27', r'%Y-%m-%d'),
            valid, product_name
        )
        self.assertEqual(result, exp)

    def test_execute_type_time(self):
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
            self._cast_datetime('2010-01-26 14:31:59', r'%Y-%m-%d %H:%M:%S'),
            account_no, Decimal(str(balance)),
            self._cast_time('13:59:59', r'%H:%M:%S')
        )
        self.assertEqual(result, exp)

    def test_execute_different_rowcounts(self):
        stmt = "insert into ACCOUNT (ACCOUNT_ID, ACCOUNT_NO, BALANCE) " \
               "values (?, ?, ?)"
        parms = (
            ( self.dbapi.Timestamp(2009, 9, 11, 14, 15, 22, 123450), 20, 13.1 ),
            ( self.dbapi.Timestamp(2009, 9, 11, 14, 15, 22, 123452), 22, 13.3 ),
            )
        with self.conn.cursor() as cursor:
            cursor.executemany(stmt, parms)
            self.assertEqual(cursor.rowcount, 2)
            parms = ( self.dbapi.Timestamp(2009, 9, 11, 14, 15, 22, 123451), 21, 13.2 )
            cursor.execute(stmt, parms)
            self.assertEqual(cursor.rowcount, 1)
            cursor.execute("select * from ACCOUNT")
            self.assertEqual(cursor.rowcount, -1)
    
    def test_execute_type_blob(self):
        stmt = "insert into ACCOUNT (ACCOUNT_ID, ACCOUNT_NO, BALANCE, " \
               "STUFF) values (?, ?, ?, ?)"
        binary_stuff = 'abcdef'.encode('UTF-8')
        account_id = self.dbapi.Timestamp(2009, 9, 11, 14, 15, 22, 123450)
        stuff = self.dbapi.Binary(binary_stuff)
        parms = (account_id, 20, 13.1, stuff)
        with self.conn.cursor() as cursor:
            cursor.execute(stmt, parms)
            stmt = "select STUFF from ACCOUNT where ACCOUNT_NO = ?"
            parms = (20, )
            cursor.execute(stmt, parms)
            result = cursor.fetchone()
        value = result[0]
        self.assertEqual(value, memoryview(binary_stuff))

class SqliteTestBase(IntegrationTestBase):

    def setUpSql(self):
        self.sql_file(os.path.join(_THIS_DIR, 'data', 'create.sql'))
        self.sql_file(os.path.join(_THIS_DIR, 'data', 'insert.sql'))

class SqlitePyTest(SqliteTestBase, unittest.TestCase):

    JDBC_SUPPORT_TEMPORAL_TYPE = True

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
        """Time type not supported by PySqlite"""

class SqliteXerialTest(SqliteTestBase, unittest.TestCase):

    JDBC_SUPPORT_TEMPORAL_TYPE = True

    def connect(self):
        #http://bitbucket.org/xerial/sqlite-jdbc
        # sqlite-jdbc-3.7.2.jar
        driver, url = 'org.sqlite.JDBC', 'jdbc:sqlite::memory:'
        properties = {
            "date_string_format": "yyyy-MM-dd HH:mm:ss"
        }
        return jaydebeapiarrow, jaydebeapiarrow.connect(driver, url, driver_args=properties)

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

class HsqldbTest(IntegrationTestBase, unittest.TestCase):

    def connect(self):
        # http://hsqldb.org/
        # hsqldb.jar
        driver, url, driver_args = ( 'org.hsqldb.jdbcDriver',
                                     'jdbc:hsqldb:mem:.',
                                     ['SA', ''] )
        return jaydebeapiarrow, jaydebeapiarrow.connect(driver, url, driver_args)

    def setUpSql(self):
        self.sql_file(os.path.join(_THIS_DIR, 'data', 'create_hsqldb.sql'))
        self.sql_file(os.path.join(_THIS_DIR, 'data', 'insert.sql'))


class PostgresTest(IntegrationTestBase, unittest.TestCase):

    def connect(self):

        import jpype

        host = os.environ.get("JY_PG_HOST", "localhost")
        port = os.environ.get("JY_PG_PORT", "5432")
        db_name = os.environ.get("JY_PG_DB", "test_db")
        user = os.environ.get("JY_PG_USER", "user")
        password = os.environ.get("JY_PG_PASSWORD", "password")

        driver, url, driver_args = (
            'org.postgresql.Driver',
            f'jdbc:postgresql://{host}:{port}/{db_name}',
            {'user': user, 'password': password}
        )

        try:
            db, conn = jaydebeapiarrow, jaydebeapiarrow.connect(driver, url, driver_args)
        except jpype.JException:
            self.skipTest("Can not connect with PostgreSQL. Please check if the instance is up and running.")
        else:
            return db, conn


    def setUpSql(self):
        self.sql_file(os.path.join(_THIS_DIR, 'data', 'create_postgres.sql'))
        self.sql_file(os.path.join(_THIS_DIR, 'data', 'insert.sql'))

    def test_execute_timestamptz_roundtrip_non_utc_session(self):
        """Test TIMESTAMPTZ read/write with a non-UTC session timezone.

        Sets the session to Australia/Sydney (UTC+10 standard / UTC+11 DST),
        inserts a naive string via SQL (interpreted as Sydney local time by PG),
        then verifies our Arrow bridge correctly normalizes to UTC on read.
        """
        with self.conn.cursor() as cursor:
            # Use a timezone with DST to make this a real test
            cursor.execute("SET TIME ZONE 'Australia/Sydney'")
            # Insert via raw SQL — PG interprets this as Sydney time
            # January = AEDT (UTC+11), so 10:30 local = 23:30 previous day UTC
            cursor.execute(
                "INSERT INTO ACCOUNT (ACCOUNT_ID, ACCOUNT_NO, BALANCE, ACCOUNT_ID_TZ) "
                "VALUES ('2024-01-15 10:30:00', 30, 5.0, '2024-01-15 10:30:00')"
            )

            # Read back via Arrow bridge — should normalize to UTC
            cursor.execute("SELECT ACCOUNT_ID, ACCOUNT_ID_TZ FROM ACCOUNT WHERE ACCOUNT_NO = 30")
            result = cursor.fetchone()

        # ACCOUNT_ID (plain TIMESTAMP) is NOT affected by timezone — returns as-is
        self.assertEqual(result[0], datetime(2024, 1, 15, 10, 30, 0))
        self.assertIsNone(result[0].tzinfo)

        # ACCOUNT_ID_TZ (TIMESTAMPTZ) is normalized to UTC by the bridge
        # 10:30 AEDT (UTC+11) = 2024-01-14 23:30:00 UTC
        self.assertEqual(result[1], datetime(2024, 1, 14, 23, 30, 0, tzinfo=timezone.utc))
        self.assertIsNotNone(result[1].tzinfo)

    def test_execute_timestamptz_roundtrip_param_binding(self):
        """Test writing a TZ-aware datetime via parameter binding and reading back."""
        # Reset to UTC for a clean parameter-binding round-trip
        with self.conn.cursor() as cursor:
            cursor.execute("SET TIME ZONE 'UTC'")
            naive_id = datetime(2024, 6, 15, 10, 30, 0)
            tz_dt = datetime(2024, 6, 15, 10, 30, 0, tzinfo=timezone.utc)
            cursor.execute(
                "INSERT INTO ACCOUNT (ACCOUNT_ID, ACCOUNT_NO, BALANCE, ACCOUNT_ID_TZ) "
                "VALUES (?, ?, ?, ?)",
                (naive_id, 31, Decimal('5.0'), tz_dt)
            )
            cursor.execute("SELECT ACCOUNT_ID, ACCOUNT_ID_TZ FROM ACCOUNT WHERE ACCOUNT_NO = 31")
            result = cursor.fetchone()

        # ACCOUNT_ID (TIMESTAMP) should be naive
        self.assertEqual(result[0], datetime(2024, 6, 15, 10, 30, 0))
        self.assertIsNone(result[0].tzinfo)
        # ACCOUNT_ID_TZ (TIMESTAMPTZ) should be timezone-aware (UTC)
        self.assertEqual(result[1], datetime(2024, 6, 15, 10, 30, 0, tzinfo=timezone.utc))
        self.assertIsNotNone(result[1].tzinfo)


class MySQLTest(IntegrationTestBase, unittest.TestCase):

    def connect(self):

        import jpype
        
        host = os.environ.get("JY_MYSQL_HOST", "localhost")
        port = os.environ.get("JY_MYSQL_PORT", "3306")
        db_name = os.environ.get("JY_MYSQL_DB", "test_db")
        user = os.environ.get("JY_MYSQL_USER", "user")
        password = os.environ.get("JY_MYSQL_PASSWORD", "password")

        driver, url, driver_args = (
            'com.mysql.cj.jdbc.Driver',
            f'jdbc:mysql://{host}:{port}/{db_name}?user={user}&password={password}',
            None
        )

        try:
            db, conn = jaydebeapiarrow, jaydebeapiarrow.connect(driver, url, driver_args)
        except jpype.JException as e:
            self.skipTest("Can not connect with MySQL. Please check if the instance is up and running.")
        else:
            return db, conn

    def setUpSql(self):
        self.sql_file(os.path.join(_THIS_DIR, 'data', 'create_mysql.sql'))
        self.sql_file(os.path.join(_THIS_DIR, 'data', 'insert.sql'))


class MSSQLTest(IntegrationTestBase, unittest.TestCase):

    def connect(self):

        import jpype

        host = os.environ.get("JY_MSSQL_HOST", "localhost")
        port = os.environ.get("JY_MSSQL_PORT", "1433")
        user = os.environ.get("JY_MSSQL_USER", "sa")
        password = os.environ.get("JY_MSSQL_PASSWORD", "Password123!")

        driver, url, driver_args = (
            'com.microsoft.sqlserver.jdbc.SQLServerDriver',
            f'jdbc:sqlserver://{host}:{port};encrypt=false;trustServerCertificate=true',
            {'user': user, 'password': password}
        )

        try:
            db, conn = jaydebeapiarrow, jaydebeapiarrow.connect(driver, url, driver_args)
        except jpype.JException:
            self.skipTest("Can not connect with MS SQL Server. Please check if the instance is up and running.")
        else:
            return db, conn

    def setUpSql(self):
        with self.conn.cursor() as cursor:
            cursor.execute("IF DB_ID('test_db') IS NULL CREATE DATABASE test_db")
            cursor.execute("USE test_db")
        self.sql_file(os.path.join(_THIS_DIR, 'data', 'create_mssql.sql'))
        self.sql_file(os.path.join(_THIS_DIR, 'data', 'insert.sql'))

    def tearDown(self):
        with self.conn.cursor() as cursor:
            cursor.execute("USE test_db")
        super().tearDown()


class TrinoTest(IntegrationTestBase, unittest.TestCase):

    def connect(self):

        import jpype

        host = os.environ.get("JY_TRINO_HOST", "localhost")
        port = os.environ.get("JY_TRINO_PORT", "8080")
        user = os.environ.get("JY_TRINO_USER", "test")

        driver, url, driver_args = (
            'io.trino.jdbc.TrinoDriver',
            f'jdbc:trino://{host}:{port}/memory/default',
            {'user': user}
        )

        try:
            db, conn = jaydebeapiarrow, jaydebeapiarrow.connect(driver, url, driver_args)
        except jpype.JException:
            self.skipTest("Can not connect with Trino. Please check if the instance is up and running.")
        else:
            return db, conn

    def setUpSql(self):
        self.sql_file(os.path.join(_THIS_DIR, 'data', 'create_trino.sql'))
        self.sql_file(os.path.join(_THIS_DIR, 'data', 'insert.sql'))

    def tearDown(self):
        with self.conn.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS ACCOUNT")
        self.conn.close()


class OracleTest(IntegrationTestBase, unittest.TestCase):

    def connect(self):

        import jpype

        host = os.environ.get("JY_ORACLE_HOST", "localhost")
        port = os.environ.get("JY_ORACLE_PORT", "1521")
        user = os.environ.get("JY_ORACLE_USER", "system")
        password = os.environ.get("JY_ORACLE_PASSWORD", "Password123!")

        driver, url, driver_args = (
            'oracle.jdbc.OracleDriver',
            f'jdbc:oracle:thin:@{host}:{port}/XEPDB1',
            {'user': user, 'password': password}
        )

        try:
            db, conn = jaydebeapiarrow, jaydebeapiarrow.connect(driver, url, driver_args)
        except jpype.JException:
            self.skipTest("Can not connect with Oracle. Please check if the instance is up and running.")
        else:
            return db, conn

    def setUpSql(self):
        self.sql_file(os.path.join(_THIS_DIR, 'data', 'create_oracle.sql'))
        self.sql_file(os.path.join(_THIS_DIR, 'data', 'insert_oracle.sql'))

    def test_execute_types(self):
        """Oracle uses NUMBER(1) instead of BOOLEAN — VALID returns int not bool."""
        stmt = "insert into ACCOUNT (ACCOUNT_ID, ACCOUNT_NO, BALANCE, " \
               "BLOCKING, DBL_COL, OPENED_AT, VALID, PRODUCT_NAME) " \
               "values (?, ?, ?, ?, ?, ?, ?, ?)"
        account_id = self.dbapi.Timestamp(2010, 1, 26, 14, 31, 59)
        account_no = 20
        balance = Decimal('1.2')
        blocking = 10.0
        dbl_col = 3.5
        opened_at = self.dbapi.Date(1908, 2, 27)
        valid = 1
        product_name = u'Savings account'
        parms = (account_id, account_no, balance, blocking, dbl_col,
                 opened_at, valid, product_name)
        with self.conn.cursor() as cursor:
            cursor.execute(stmt, parms)
            stmt = "select ACCOUNT_ID, ACCOUNT_NO, BALANCE, BLOCKING, " \
                "DBL_COL, OPENED_AT, VALID, PRODUCT_NAME " \
                "from ACCOUNT where ACCOUNT_NO = ?"
            parms = (20, )
            cursor.execute(stmt, parms)
            result = cursor.fetchone()
        exp = (
            self._cast_datetime('2010-01-26 14:31:59', r'%Y-%m-%d %H:%M:%S'),
            account_no, balance, blocking, dbl_col,
            self._cast_date('1908-02-27', r'%Y-%m-%d'),
            valid, product_name
        )
        self.assertEqual(result, exp)

    def test_execute_type_time(self):
        """Oracle has no native TIME type — OPENED_AT_TIME is TIMESTAMP."""
        stmt = "insert into ACCOUNT (ACCOUNT_ID, ACCOUNT_NO, BALANCE, " \
               "OPENED_AT_TIME) " \
               "values (?, ?, ?, ?)"
        account_id = self.dbapi.Timestamp(2010, 1, 26, 14, 31, 59)
        account_no = 20
        balance = 1.2
        opened_at_time = self.dbapi.Timestamp(1970, 1, 1, 13, 59, 59)
        parms = (account_id, account_no, balance, opened_at_time)
        with self.conn.cursor() as cursor:
            cursor.execute(stmt, parms)
            stmt = "select ACCOUNT_ID, ACCOUNT_NO, BALANCE, OPENED_AT_TIME " \
                "from ACCOUNT where ACCOUNT_NO = ?"
            parms = (20, )
            cursor.execute(stmt, parms)
            result = cursor.fetchone()

        exp = (
            self._cast_datetime('2010-01-26 14:31:59', r'%Y-%m-%d %H:%M:%S'),
            account_no, Decimal(str(balance)),
            self._cast_datetime('1970-01-01 13:59:59', r'%Y-%m-%d %H:%M:%S')
        )
        self.assertEqual(result, exp)


class DB2Test(IntegrationTestBase, unittest.TestCase):

    def connect(self):

        import jpype

        host = os.environ.get("JY_DB2_HOST", "localhost")
        port = os.environ.get("JY_DB2_PORT", "50000")
        user = os.environ.get("JY_DB2_USER", "db2inst1")
        password = os.environ.get("JY_DB2_PASSWORD", "Password123!")

        driver, url, driver_args = (
            'com.ibm.db2.jcc.DB2Driver',
            f'jdbc:db2://{host}:{port}/test_db',
            {'user': user, 'password': password}
        )

        try:
            db, conn = jaydebeapiarrow, jaydebeapiarrow.connect(driver, url, driver_args)
        except jpype.JException:
            self.skipTest("Can not connect with DB2. Please check if the instance is up and running.")
        else:
            return db, conn

    def setUpSql(self):
        self.sql_file(os.path.join(_THIS_DIR, 'data', 'create_db2.sql'))
        self.sql_file(os.path.join(_THIS_DIR, 'data', 'insert.sql'))

    def test_execute_types(self):
        """DB2 uses SMALLINT instead of BOOLEAN — VALID returns int not bool."""
        stmt = "insert into ACCOUNT (ACCOUNT_ID, ACCOUNT_NO, BALANCE, " \
               "BLOCKING, DBL_COL, OPENED_AT, VALID, PRODUCT_NAME) " \
               "values (?, ?, ?, ?, ?, ?, ?, ?)"
        account_id = self.dbapi.Timestamp(2010, 1, 26, 14, 31, 59)
        account_no = 20
        balance = Decimal('1.2')
        blocking = 10.0
        dbl_col = 3.5
        opened_at = self.dbapi.Date(1908, 2, 27)
        valid = 1
        product_name = u'Savings account'
        parms = (account_id, account_no, balance, blocking, dbl_col,
                 opened_at, valid, product_name)
        with self.conn.cursor() as cursor:
            cursor.execute(stmt, parms)
            stmt = "select ACCOUNT_ID, ACCOUNT_NO, BALANCE, BLOCKING, " \
                "DBL_COL, OPENED_AT, VALID, PRODUCT_NAME " \
                "from ACCOUNT where ACCOUNT_NO = ?"
            parms = (20, )
            cursor.execute(stmt, parms)
            result = cursor.fetchone()
        exp = (
            self._cast_datetime('2010-01-26 14:31:59', r'%Y-%m-%d %H:%M:%S'),
            account_no, balance, blocking, dbl_col,
            self._cast_date('1908-02-27', r'%Y-%m-%d'),
            valid, product_name
        )
        self.assertEqual(result, exp)


class DrillTest(IntegrationTestBase, unittest.TestCase):

    def connect(self):

        import jpype

        host = os.environ.get("JY_DRILL_HOST", "localhost")
        port = os.environ.get("JY_DRILL_PORT", "31010")

        driver, url, driver_args = (
            'org.apache.drill.jdbc.Driver',
            f'jdbc:drill:drillbit={host}:{port}',
            None
        )

        try:
            db, conn = jaydebeapiarrow, jaydebeapiarrow.connect(driver, url, driver_args)
        except jpype.JException:
            self.skipTest("Can not connect with Drill. Please check if the instance is up and running.")
        else:
            return db, conn

    def setUpSql(self):
        self.sql_file(os.path.join(_THIS_DIR, 'data', 'create_drill.sql'))

    def tearDown(self):
        with self.conn.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS dfs.tmp.account")
        self.conn.close()

    def _query_table(self, cursor):
        cursor.execute("select ACCOUNT_ID, ACCOUNT_NO, BALANCE, BLOCKING "
                       "from dfs.tmp.account")

    def test_executemany(self):
        """Drill has no INSERT INTO ... VALUES — skip executemany test."""
        self.skipTest("Drill does not support INSERT INTO ... VALUES")

    def test_execute_types(self):
        """Drill has no INSERT INTO ... VALUES — skip types test."""
        self.skipTest("Drill does not support INSERT INTO ... VALUES")

    def test_execute_type_time(self):
        """Drill has no INSERT INTO ... VALUES — skip time test."""
        self.skipTest("Drill does not support INSERT INTO ... VALUES")

    def test_execute_type_blob(self):
        """Drill has no INSERT INTO ... VALUES — skip blob test."""
        self.skipTest("Drill does not support INSERT INTO ... VALUES")

    def test_execute_different_rowcounts(self):
        """Drill has no INSERT INTO ... VALUES — skip rowcount test."""
        self.skipTest("Drill does not support INSERT INTO ... VALUES")

    def test_execute_reset_description_without_execute_result(self):
        """Drill has no DELETE — verify description reset with SELECT only."""
        with self.conn.cursor() as cursor:
            cursor.execute("select * from dfs.tmp.account")
            self.assertIsNotNone(cursor.description)
            cursor.fetchone()

    def test_execute_and_fetch(self):
        with self.conn.cursor() as cursor:
            cursor.execute("select ACCOUNT_ID, ACCOUNT_NO, BALANCE, BLOCKING "
                           "from dfs.tmp.account")
            result = cursor.fetchall()
        self.assertEqual(result, [
            (
            self._cast_datetime('2009-09-10 14:15:22.123', r'%Y-%m-%d %H:%M:%S.%f'),
            18, Decimal('12.40'), None),
            (
            self._cast_datetime('2009-09-11 14:15:22.123', r'%Y-%m-%d %H:%M:%S.%f'),
            19, Decimal('12.90'), Decimal('1'))
        ])

    def test_execute_and_fetchone(self):
        with self.conn.cursor() as cursor:
            cursor.execute("select ACCOUNT_ID, ACCOUNT_NO, BALANCE, BLOCKING "
                           "from dfs.tmp.account order by ACCOUNT_NO")
            result = cursor.fetchone()
        self.assertEqual(result, (
            self._cast_datetime('2009-09-10 14:15:22.123', r'%Y-%m-%d %H:%M:%S.%f'),
            18, Decimal('12.40'), None))
        cursor.close()

    def test_execute_and_fetchone_consecutive(self):
        with self.conn.cursor() as cursor:
            cursor.execute("select ACCOUNT_ID, ACCOUNT_NO, BALANCE, BLOCKING "
                           "from dfs.tmp.account order by ACCOUNT_NO")
            result1 = cursor.fetchone()
            result2 = cursor.fetchone()

        self.assertEqual(result1, (
            self._cast_datetime('2009-09-10 14:15:22.123', r'%Y-%m-%d %H:%M:%S.%f'),
            18, Decimal('12.40'), None))

        self.assertEqual(result2, (
            self._cast_datetime('2009-09-11 14:15:22.123', r'%Y-%m-%d %H:%M:%S.%f'),
            19, Decimal('12.90'), Decimal('1')))

    def test_execute_and_fetchmany(self):
        with self.conn.cursor() as cursor:
            cursor.execute("select ACCOUNT_ID, ACCOUNT_NO, BALANCE, BLOCKING "
                           "from dfs.tmp.account order by ACCOUNT_NO")
            result = cursor.fetchmany()
        self.assertEqual(result, [
            (
            self._cast_datetime('2009-09-10 14:15:22.123', r'%Y-%m-%d %H:%M:%S.%f'),
            18, Decimal('12.40'), None)
        ])


class PropertiesDriverArgsPassingTest(unittest.TestCase):

    def test_connect_with_sequence(self):
        driver, url, driver_args = ( 'org.hsqldb.jdbcDriver',
                                     'jdbc:hsqldb:mem:.',
                                     ['SA', ''] )
        c = jaydebeapiarrow.connect(driver, url, driver_args)
        c.close()

    def test_connect_with_properties(self):
        driver, url, driver_args = ( 'org.hsqldb.jdbcDriver',
                                     'jdbc:hsqldb:mem:.',
                                     {'user': 'SA', 'password': '' } )
        c = jaydebeapiarrow.connect(driver, url, driver_args)
        c.close()
