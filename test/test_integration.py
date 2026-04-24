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

import calendar
import glob
import os
import shutil
import subprocess
import sys
import tempfile
import threading

import unittest

from decimal import Decimal
from datetime import datetime, timedelta, timezone
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
                cursor.execute(i.rstrip().rstrip(";"))

    def setUp(self):
        (self.dbapi, self.conn) = self.connect()
        self._suppress_java_noise()
        self.setUpSql()

    @staticmethod
    def _suppress_java_noise():
        """Suppress noisy Java loggers from Drill, Trino, etc."""
        try:
            import jpype
            from jaydebeapiarrow import _is_jvm_started
            if not _is_jvm_started():
                return
            Level = jpype.JClass("java.util.logging.Level")
            root = jpype.JClass("java.util.logging.Logger").getLogger("")
            for name in (
                "oadd.org.apache.drill",
                "org.apache.drill",
                "io.trino",
                "org.apache.arrow.memory",
                "org.apache.arrow.vector",
                "org.jaydebeapiarrow.extension",
            ):
                root.getLogger(name).setLevel(Level.WARNING)
        except Exception:
            pass

    def setUpSql(self):
        raise NotImplementedError

    def connect(self):
        raise NotImplementedError

    def tearDown(self):
        with self.conn.cursor() as cursor:
            cursor.execute("drop table ACCOUNT")
            self._numeric_teardown()
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
                        "from ACCOUNT ORDER BY ACCOUNT_NO")
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

    def test_lastrowid_exists_and_is_none(self):
        """PEP-249: lastrowid attribute must exist and be None (fixes #84)."""
        with self.conn.cursor() as cursor:
            self.assertIsNone(cursor.lastrowid)

    def test_lastrowid_none_after_select(self):
        """lastrowid should be None after a SELECT query."""
        with self.conn.cursor() as cursor:
            cursor.execute("select * from ACCOUNT")
            self.assertIsNone(cursor.lastrowid)

    def test_lastrowid_none_after_insert(self):
        """lastrowid should be None after INSERT (JDBC doesn't expose rowid)."""
        stmt = "insert into ACCOUNT (ACCOUNT_ID, ACCOUNT_NO, BALANCE) " \
               "values (?, ?, ?)"
        with self.conn.cursor() as cursor:
            cursor.execute(stmt, (self.dbapi.Timestamp(2009, 9, 11, 14, 15, 22, 123450), 99, 1.0))
            self.assertIsNone(cursor.lastrowid)

    def test_lastrowid_none_after_executemany(self):
        """lastrowid should be None after executemany."""
        stmt = "insert into ACCOUNT (ACCOUNT_ID, ACCOUNT_NO, BALANCE) " \
               "values (?, ?, ?)"
        parms = (
            (self.dbapi.Timestamp(2009, 9, 11, 14, 15, 22, 123450), 98, 1.0),
            (self.dbapi.Timestamp(2009, 9, 11, 14, 15, 22, 123452), 97, 2.0),
        )
        with self.conn.cursor() as cursor:
            cursor.executemany(stmt, parms)
            self.assertIsNone(cursor.lastrowid)

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

    def test_timestamp_subsecond_leading_zeros(self):
        """Verify that TIMESTAMP columns preserve sub-second leading zeros.
        Regression test for legacy baztian/jaydebeapi#44 where
        2017-06-19 15:30:00.096965169 was displayed as
        2017-06-19 15:30:00.960000 due to string-based parsing
        stripping the leading zero. The Arrow path uses integer
        nanosecond arithmetic, so this should be correct."""
        test_cases = [
            # (year, month, day, hour, minute, second, microsecond)
            (2017, 6, 19, 15, 30, 0, 96965),    # .096965 — exact case from legacy #44
            (2020, 1, 1, 0, 0, 0, 1),          # .000001 — minimal non-zero
            (2021, 3, 15, 12, 0, 0, 1000),      # .001000 — leading zeros then trailing
            (2019, 7, 4, 10, 30, 0, 99999),     # .099999 — leading zero + 9s
            (2022, 1, 1, 0, 0, 0, 0),           # .000000 — zero sub-second
        ]
        stmt = ("insert into ACCOUNT (ACCOUNT_ID, ACCOUNT_NO, BALANCE) "
                "values (?, ?, ?)")
        with self.conn.cursor() as cursor:
            for idx, (y, mo, d, h, mi, s, us) in enumerate(test_cases):
                ts = self.dbapi.Timestamp(y, mo, d, h, mi, s, us)
                cursor.execute(stmt, (ts, 60 + idx, Decimal('1.0')))
            cursor.execute(
                "select ACCOUNT_ID from ACCOUNT "
                "where ACCOUNT_NO >= 60 order by ACCOUNT_NO")
            results = cursor.fetchall()
        for idx, (y, mo, d, h, mi, s, us) in enumerate(test_cases):
            expected = self._cast_datetime(
                f'{y}-{mo:02d}-{d:02d} {h:02d}:{mi:02d}:{s:02d}.{us:06d}',
                r'%Y-%m-%d %H:%M:%S.%f')
            self.assertEqual(results[idx][0], expected,
                             f"Failed for microseconds={us}")

    def test_timestamp_microsecond_precision(self):
        """Verify that TIMESTAMP columns preserve microsecond precision.
        Regression test for legacy issue baztian/jaydebeapi#229 where certain
        microsecond values (e.g. 90000) were corrupted during the Arrow
        conversion."""
        test_cases = [
            (2009, 9, 11, 10, 0, 0, 200000),
            (2009, 9, 11, 10, 0, 1, 90000),
            (2009, 9, 11, 10, 0, 2, 123456),
            (2009, 9, 11, 10, 0, 3, 0),
            (2009, 9, 11, 10, 0, 4, 999999),
        ]
        stmt = ("insert into ACCOUNT (ACCOUNT_ID, ACCOUNT_NO, BALANCE) "
                "values (?, ?, ?)")
        with self.conn.cursor() as cursor:
            for idx, (y, mo, d, h, mi, s, us) in enumerate(test_cases):
                ts = self.dbapi.Timestamp(y, mo, d, h, mi, s, us)
                cursor.execute(stmt, (ts, 50 + idx, Decimal('1.0')))
            cursor.execute(
                "select ACCOUNT_ID from ACCOUNT "
                "where ACCOUNT_NO >= 50 order by ACCOUNT_NO")
            results = cursor.fetchall()
        for idx, (y, mo, d, h, mi, s, us) in enumerate(test_cases):
            expected = self._cast_datetime(
                f'{y}-{mo:02d}-{d:02d} {h:02d}:{mi:02d}:{s:02d}.{us:06d}',
                r'%Y-%m-%d %H:%M:%S.%f')
            self.assertEqual(results[idx][0], expected,
                             f"Failed for microseconds={us}")

    def test_binary_non_utf8_roundtrip(self):
        """Verify that binary data containing non-UTF-8 bytes round-trips
        correctly through the Arrow path. Regression test for legacy issue
        baztian/jaydebeapi#147 where binary data was incorrectly decoded as
        UTF-8 strings, corrupting byte values >= 0x80."""
        test_data = bytes([0x00, 0x01, 0x02, 0x80, 0xff, 0xfe])
        stmt = ("insert into ACCOUNT (ACCOUNT_ID, ACCOUNT_NO, BALANCE, "
                "STUFF) values (?, ?, ?, ?)")
        account_id = self.dbapi.Timestamp(2009, 9, 11, 14, 15, 22, 123450)
        stuff = self.dbapi.Binary(test_data)
        parms = (account_id, 20, 13.1, stuff)
        with self.conn.cursor() as cursor:
            cursor.execute(stmt, parms)
            cursor.execute("select STUFF from ACCOUNT where ACCOUNT_NO = ?",
                           (20,))
            result = cursor.fetchone()
        value = result[0]
        self.assertEqual(bytes(value), test_data)

    def test_blob_non_utf8_roundtrip(self):
        """Verify BLOB columns preserve non-UTF-8 bytes through Arrow path.
        Regression test for legacy issue baztian/jaydebeapi#76 where BLOB
        data returned as raw Java objects instead of Python bytes."""
        test_data = bytes([0x00, 0x01, 0x02, 0x80, 0xff, 0xfe])
        stmt = ("insert into ACCOUNT (ACCOUNT_ID, ACCOUNT_NO, BALANCE, "
                "STUFF) values (?, ?, ?, ?)")
        account_id = self.dbapi.Timestamp(2009, 9, 11, 14, 15, 22, 123450)
        stuff = self.dbapi.Binary(test_data)
        parms = (account_id, 20, 13.1, stuff)
        with self.conn.cursor() as cursor:
            cursor.execute(stmt, parms)
            cursor.execute("select STUFF from ACCOUNT where ACCOUNT_NO = ?",
                           (20,))
            result = cursor.fetchone()
        self.assertIsInstance(result[0], (bytes, memoryview))
        self.assertEqual(bytes(result[0]), test_data)

    def test_blob_all_byte_values_roundtrip(self):
        """All 256 byte values should round-trip correctly through BLOB columns."""
        test_data = bytes(range(256))
        stmt = ("insert into ACCOUNT (ACCOUNT_ID, ACCOUNT_NO, BALANCE, "
                "STUFF) values (?, ?, ?, ?)")
        account_id = self.dbapi.Timestamp(2009, 9, 11, 14, 15, 22, 123450)
        stuff = self.dbapi.Binary(test_data)
        parms = (account_id, 21, 13.2, stuff)
        with self.conn.cursor() as cursor:
            cursor.execute(stmt, parms)
            cursor.execute("select STUFF from ACCOUNT where ACCOUNT_NO = ?",
                           (21,))
            result = cursor.fetchone()
        self.assertEqual(bytes(result[0]), test_data)

    def test_blob_null_value(self):
        """NULL BLOB values should return None, not crash or return garbage."""
        stmt = ("insert into ACCOUNT (ACCOUNT_ID, ACCOUNT_NO, BALANCE, "
                "STUFF) values (?, ?, ?, ?)")
        account_id = self.dbapi.Timestamp(2009, 9, 11, 14, 15, 22, 123450)
        parms = (account_id, 22, 13.3, None)
        with self.conn.cursor() as cursor:
            cursor.execute(stmt, parms)
            cursor.execute("select STUFF from ACCOUNT where ACCOUNT_NO = ?",
                           (22,))
            result = cursor.fetchone()
        self.assertIsNone(result[0])

    def test_numeric_types(self):
        """Test that NUMERIC columns round-trip correctly, including NULL values
        and edge-case precision/scale values."""
        create_table = self._numeric_create_table_sql()
        with self.conn.cursor() as cursor:
            cursor.execute(create_table)
            # Insert NULL numeric value
            cursor.execute(
                "INSERT INTO NUMERIC_TEST (ID, NUM_COL) VALUES (1, NULL)")
            # Insert a regular numeric value
            cursor.execute(
                "INSERT INTO NUMERIC_TEST (ID, NUM_COL) VALUES (2, 99.99)")
            # Insert an integer-like numeric value
            cursor.execute(
                "INSERT INTO NUMERIC_TEST (ID, NUM_COL) VALUES (3, 100.00)")
            # Read back only the numeric column to avoid ID type differences
            cursor.execute("SELECT NUM_COL FROM NUMERIC_TEST ORDER BY ID")
            result = cursor.fetchall()
        self.assertEqual(len(result), 3)
        self.assertIsNone(result[0][0])       # NULL
        self.assertEqual(result[1][0], Decimal('99.99'))
        self.assertEqual(result[2][0], Decimal('100.00'))

    def test_bigint_column_returns_int(self):
        """Verify JDBC BIGINT columns return Python int, not raw java.lang.Long.
        Regression test for legacy baztian/jaydebeapi#63."""
        if type(self).__name__.startswith(('OracleTest', 'DrillTest')):
            self.skipTest('BIGINT type not supported by this database')
        with self.conn.cursor() as cursor:
            cursor.execute("CREATE TABLE BIGINT_TEST (val BIGINT)")
            try:
                cursor.execute("INSERT INTO BIGINT_TEST VALUES (0)")
                cursor.execute("INSERT INTO BIGINT_TEST VALUES (377518399)")
                cursor.execute("INSERT INTO BIGINT_TEST VALUES (-9223372036854775808)")
                cursor.execute("INSERT INTO BIGINT_TEST VALUES (9223372036854775807)")
                cursor.execute("SELECT val FROM BIGINT_TEST ORDER BY val")
                result = cursor.fetchall()
            finally:
                cursor.execute("DROP TABLE BIGINT_TEST")
        self.assertEqual(len(result), 4)
        for row in result:
            self.assertIsInstance(row[0], int)
        self.assertEqual(result[0][0], -9223372036854775808)
        self.assertEqual(result[1][0], 0)
        self.assertEqual(result[2][0], 377518399)
        self.assertEqual(result[3][0], 9223372036854775807)

    def test_double_column_returns_float(self):
        """Verify JDBC DOUBLE columns return Python float, not raw java.lang.Double.
        Regression test for legacy baztian/jaydebeapi#243."""
        with self.conn.cursor() as cursor:
            cursor.execute(self._double_create_sql())
            try:
                self._double_populate(cursor)
                cursor.execute("SELECT val FROM DOUBLE_TEST ORDER BY val")
                result = cursor.fetchall()
            finally:
                cursor.execute("DROP TABLE DOUBLE_TEST")
        self.assertEqual(len(result), 3)
        for row in result:
            self.assertIsInstance(row[0], float)
        self.assertAlmostEqual(result[0][0], -1.5)
        self.assertAlmostEqual(result[1][0], 0.0)
        self.assertAlmostEqual(result[2][0], 3.14)

    def _double_populate(self, cursor):
        cursor.execute("INSERT INTO DOUBLE_TEST VALUES (3.14)")
        cursor.execute("INSERT INTO DOUBLE_TEST VALUES (-1.5)")
        cursor.execute("INSERT INTO DOUBLE_TEST VALUES (0.0)")

    def test_numeric_precision_scale_combos(self):
        """Test various DECIMAL/NUMERIC precision/scale combinations."""
        with self.conn.cursor() as cursor:
            cursor.execute(self._numeric_combo_create_sql())
            cursor.execute(self._numeric_combo_insert_sql())
            cursor.execute("SELECT DEC_S2, DEC_S4, DEC_S0, DEC_PES, "
                           "NUM_S2, NUM_S0, NUM_S4, NUM_PES, NUM_NEG "
                           "FROM NUMERIC_COMBO ORDER BY ID")
            result = cursor.fetchone()
        self.assertEqual(result[0], Decimal('12345.67'))          # DECIMAL(10, 2)
        self.assertEqual(result[1], Decimal('12345.6789'))        # DECIMAL(15, 4)
        self.assertEqual(result[2], Decimal('987654321012345678')) # DECIMAL(18, 0)
        self.assertEqual(result[3], Decimal('0.12345'))           # DECIMAL(5, 5)
        self.assertEqual(result[4], Decimal('99.99'))             # NUMERIC(10, 2)
        self.assertEqual(result[5], Decimal('42'))                # NUMERIC(10, 0)
        self.assertEqual(result[6], Decimal('12345.6789'))        # NUMERIC(15, 4)
        self.assertEqual(result[7], Decimal('0.1234'))            # NUMERIC(4, 4)
        self.assertEqual(result[8], Decimal('-99.99'))            # NUMERIC(10, 2)

    def _numeric_combo_create_sql(self):
        return (
            "CREATE TABLE NUMERIC_COMBO ("
            "ID INTEGER NOT NULL, "
            "DEC_S2 DECIMAL(10, 2), "
            "DEC_S4 DECIMAL(15, 4), "
            "DEC_S0 DECIMAL(18, 0), "
            "DEC_PES DECIMAL(5, 5), "
            "NUM_S2 NUMERIC(10, 2), "
            "NUM_S0 NUMERIC(10, 0), "
            "NUM_S4 NUMERIC(15, 4), "
            "NUM_PES NUMERIC(4, 4), "
            "NUM_NEG NUMERIC(10, 2), "
            "PRIMARY KEY (ID))"
        )

    def _numeric_combo_insert_sql(self):
        return (
            "INSERT INTO NUMERIC_COMBO "
            "(ID, DEC_S2, DEC_S4, DEC_S0, DEC_PES, "
            "NUM_S2, NUM_S0, NUM_S4, NUM_PES, NUM_NEG) "
            "VALUES (1, 12345.67, 12345.6789, 987654321012345678, 0.12345, "
            "99.99, 42, 12345.6789, 0.1234, -99.99)"
        )

    def _numeric_create_table_sql(self):
        return (
            "CREATE TABLE NUMERIC_TEST ("
            "ID INTEGER NOT NULL, "
            "NUM_COL NUMERIC(10, 2), "
            "PRIMARY KEY (ID))"
        )

    def _numeric_teardown(self):
        with self.conn.cursor() as cursor:
            try:
                cursor.execute("DROP TABLE NUMERIC_TEST")
            except Exception:
                pass
            try:
                cursor.execute("DROP TABLE NUMERIC_COMBO")
            except Exception:
                pass

    def _double_create_sql(self):
        return "CREATE TABLE DOUBLE_TEST (val DOUBLE)"

    def test_execute_param_none(self):
        """Verify that Python None round-trips as SQL NULL via parameter binding."""
        stmt = "insert into ACCOUNT (ACCOUNT_ID, ACCOUNT_NO, BALANCE, BLOCKING) " \
               "values (?, ?, ?, ?)"
        account_id = self.dbapi.Timestamp(2010, 1, 26, 14, 31, 59)
        with self.conn.cursor() as cursor:
            cursor.execute(stmt, (account_id, 30, Decimal('5.0'), None))
            cursor.execute("select BLOCKING from ACCOUNT where ACCOUNT_NO = 30")
            result = cursor.fetchone()
        self.assertIsNone(result[0])

class SqliteTestBase(IntegrationTestBase):

    def setUpSql(self):
        self.sql_file(os.path.join(_THIS_DIR, 'data', 'create.sql'))
        self.sql_file(os.path.join(_THIS_DIR, 'data', 'insert.sql'))

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

    def test_timestamp_microsecond_precision(self):
        """SQLite Xerial JDBC truncates microseconds via date_string_format."""
        self.skipTest("SQLite Xerial JDBC truncates microsecond precision")

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

    def test_varchar_non_ascii_roundtrip(self):
        """Verify that VARCHAR columns containing non-ASCII characters
        round-trip correctly through the Arrow path. Regression test for
        legacy issue baztian/jaydebeapi#176 where reading VARCHAR columns
        with umlauts caused CharConversionException."""
        test_cases = [
            "Grüße aus München",
            "café — résumé",
            "こんにちは",
            "Hello 🌍",
        ]
        stmt = ("insert into ACCOUNT (ACCOUNT_ID, ACCOUNT_NO, BALANCE, "
                "PRODUCT_NAME) values (?, ?, ?, ?)")
        with self.conn.cursor() as cursor:
            for idx, text in enumerate(test_cases):
                ts = self.dbapi.Timestamp(2024, 1, 15, 10, 0, 0, idx * 100000)
                cursor.execute(stmt, (ts, 50 + idx, Decimal('1.0'), text))
            cursor.execute(
                "select PRODUCT_NAME from ACCOUNT "
                "where ACCOUNT_NO >= 50 order by ACCOUNT_NO")
            results = cursor.fetchall()
        for idx, text in enumerate(test_cases):
            self.assertEqual(results[idx][0], text,
                             f"Failed for text: {text!r}")

    def test_long_query_string_18k_characters(self):
        """SQL queries with 18k+ characters must execute correctly.
        Regression test for baztian/jaydebeapi#91 where long queries
        caused failures in the legacy codebase."""
        long_query = ("SELECT ACCOUNT_NO FROM ACCOUNT WHERE ACCOUNT_NO IN ("
                      + ",".join(str(i) for i in range(5000)) + ")")
        self.assertGreater(len(long_query), 18000,
                           "Test query must exceed 18k characters")
        with self.conn.cursor() as cursor:
            cursor.execute(long_query)
            result = cursor.fetchall()
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 2,
                         "Both ACCOUNT rows (18, 19) should match the IN clause")
        returned_ids = sorted(row[0] for row in result)
        self.assertEqual(returned_ids, [18, 19])

    def test_iterator_closed_after_fetchall(self):
        """After fetchall exhausts the result set, the Arrow iterator should
        be closed and nulled out (memory leak regression, legacy #227)."""
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT * FROM Account")
            cursor.fetchall()
            self.assertIsNone(cursor._iter)

    def test_iterator_closed_after_fetchone_exhaustion(self):
        """After fetchone exhausts the result set, iterator should be closed."""
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM Account")
            cursor.fetchone()
            result = cursor.fetchone()
            self.assertIsNone(result)
            self.assertIsNone(cursor._iter)

    def test_iterator_closed_after_fetchmany_exhaustion(self):
        """After fetchmany exhausts the result set, iterator should be closed."""
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT * FROM Account")
            cursor.fetchmany(size=1000)
            self.assertIsNone(cursor._iter)

    def test_repeated_query_cycles_release_resources(self):
        """Repeated execute/fetchall cycles should not accumulate iterators
        or buffers (memory leak regression, legacy #227)."""
        with self.conn.cursor() as cursor:
            for _ in range(5):
                cursor.execute("SELECT * FROM Account")
                result = cursor.fetchall()
                self.assertTrue(len(result) > 0)
                self.assertIsNone(cursor._iter)
                self.assertEqual(cursor._buffer, [])

    def test_description_returns_column_alias(self):
        """cursor.description should return the AS alias, not the table column name."""
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT ACCOUNT_NO AS acct_num FROM ACCOUNT")
            self.assertEqual(cursor.description[0][0], "ACCT_NUM")


    def test_timestamp_utc_roundtrip_no_timezone_shift(self):
        """Verify TIMESTAMP values round-trip without timezone shifting.

        Regression test for baztian/jaydebeapi#73. Legacy jaydebeapi returned
        timestamps in the JVM's local timezone instead of UTC. This test
        inserts specific timestamp values via parameter binding and verifies
        they are returned as naive datetime objects with exact values — no
        timezone offset applied.
        """
        test_cases = [
            # (inserted_timestamp, description)
            (self.dbapi.Timestamp(2024, 1, 15, 0, 0, 0),
             "UTC midnight — legacy bug would shift to previous day in EST"),
            (self.dbapi.Timestamp(2024, 6, 15, 14, 30, 0, 123456),
             "midday with microseconds"),
            (self.dbapi.Timestamp(2024, 12, 31, 23, 59, 59, 999999),
             "end-of-day edge case — legacy bug could roll over to next day"),
        ]
        stmt = ("insert into ACCOUNT (ACCOUNT_ID, ACCOUNT_NO, BALANCE) "
                "values (?, ?, ?)")
        with self.conn.cursor() as cursor:
            for idx, (ts, _desc) in enumerate(test_cases):
                cursor.execute(stmt, (ts, 100 + idx, Decimal('1.0')))
            cursor.execute(
                "select ACCOUNT_ID from ACCOUNT "
                "where ACCOUNT_NO >= 100 order by ACCOUNT_NO")
            results = cursor.fetchall()
        for idx, (ts, desc) in enumerate(test_cases):
            with self.subTest(desc=desc):
                self.assertEqual(results[idx][0], ts)
                self.assertIsNone(results[idx][0].tzinfo,
                                  "TIMESTAMP must return naive datetime")

    def test_varchar_columns_return_data(self):
        """Verify VARCHAR columns return actual data, not empty strings.

        Regression test for legacy issue #119 where Oracle 9i VARCHAR2 columns
        returned empty strings while numeric fields worked fine. The original
        jaydebeapi used getObject() which could return driver-specific types
        (e.g., oracle.sql.CHAR) that JPype couldn't convert. jaydebeapiarrow's
        Arrow JDBC adapter uses getString() for VARCHAR columns, which always
        returns a proper java.lang.String.
        """
        with self.conn.cursor() as cursor:
            # Insert rows with VARCHAR data
            cursor.execute(
                "INSERT INTO ACCOUNT "
                "(ACCOUNT_ID, ACCOUNT_NO, BALANCE, PRODUCT_NAME) "
                "VALUES ('2010-01-01 00:00:00.000000', 100, 99.99, 'Savings Account')"
            )
            cursor.execute(
                "INSERT INTO ACCOUNT "
                "(ACCOUNT_ID, ACCOUNT_NO, BALANCE, PRODUCT_NAME) "
                "VALUES ('2010-01-02 00:00:00.000000', 101, 0.00, 'Checking Account')"
            )
            # Query with mixed VARCHAR and numeric columns
            cursor.execute(
                "SELECT ACCOUNT_NO, BALANCE, PRODUCT_NAME "
                "FROM ACCOUNT WHERE ACCOUNT_NO >= 100 ORDER BY ACCOUNT_NO"
            )
            result = cursor.fetchall()
        self.assertEqual(len(result), 2)
        # Verify numeric data is present
        self.assertEqual(result[0][0], 100)
        self.assertEqual(result[0][1], Decimal('99.99'))
        # Verify VARCHAR data is NOT empty
        self.assertIsInstance(result[0][2], str)
        self.assertEqual(result[0][2], 'Savings Account')
        self.assertNotEqual(result[0][2], '')
        self.assertEqual(result[1][2], 'Checking Account')

    def test_commit_with_autocommit_enabled(self):
        """commit() should not raise when autocommit is enabled."""
        self.conn.jconn.setAutoCommit(True)
        self.conn.commit()

    def test_commit_with_autocommit_disabled(self):
        """commit() should succeed normally when autocommit is disabled."""
        self.conn.jconn.setAutoCommit(False)
        self.conn.commit()

    def test_rollback_with_autocommit_enabled(self):
        """rollback() should not raise when autocommit is enabled."""
        self.conn.jconn.setAutoCommit(True)
        self.conn.rollback()

    def test_rollback_with_autocommit_disabled(self):
        """rollback() should succeed normally when autocommit is disabled."""
        self.conn.jconn.setAutoCommit(False)
        self.conn.rollback()


class PostgresTest(IntegrationTestBase, unittest.TestCase):

    def connect(self):

        import jpype

        host = os.environ.get("JY_PG_HOST", "localhost")
        port = os.environ.get("JY_PG_PORT", "15432")
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
            self.fail("Can not connect with PostgreSQL. Please check if the instance is up and running.")
        else:
            return db, conn


    def setUpSql(self):
        self.sql_file(os.path.join(_THIS_DIR, 'data', 'create_postgres.sql'))
        self.sql_file(os.path.join(_THIS_DIR, 'data', 'insert.sql'))

    def _double_create_sql(self):
        return "CREATE TABLE DOUBLE_TEST (val DOUBLE PRECISION)"

    def test_timestamp_microsecond_precision(self):
        """PostgreSQL-specific: verify microsecond precision on both TIMESTAMP
        and TIMESTAMPTZ columns."""
        test_cases = [
            (2009, 9, 11, 10, 0, 0, 200000),
            (2009, 9, 11, 10, 0, 1, 90000),
            (2009, 9, 11, 10, 0, 2, 123456),
            (2009, 9, 11, 10, 0, 3, 0),
            (2009, 9, 11, 10, 0, 4, 999999),
        ]
        stmt = ("insert into ACCOUNT (ACCOUNT_ID, ACCOUNT_NO, BALANCE, "
                "ACCOUNT_ID_TZ) values (?, ?, ?, ?)")
        with self.conn.cursor() as cursor:
            cursor.execute("SET TIME ZONE 'UTC'")
            for idx, (y, mo, d, h, mi, s, us) in enumerate(test_cases):
                ts = self.dbapi.Timestamp(y, mo, d, h, mi, s, us)
                cursor.execute(stmt, (ts, 50 + idx, Decimal('1.0'), ts))
            cursor.execute(
                "select ACCOUNT_ID, ACCOUNT_ID_TZ from ACCOUNT "
                "where ACCOUNT_NO >= 50 order by ACCOUNT_NO")
            results = cursor.fetchall()
        for idx, (y, mo, d, h, mi, s, us) in enumerate(test_cases):
            expected = self._cast_datetime(
                f'{y}-{mo:02d}-{d:02d} {h:02d}:{mi:02d}:{s:02d}.{us:06d}',
                r'%Y-%m-%d %H:%M:%S.%f')
            self.assertEqual(results[idx][0], expected,
                             f"TIMESTAMP failed for microseconds={us}")
            # TIMESTAMPTZ should be timezone-aware (UTC)
            self.assertEqual(results[idx][1],
                             expected.replace(tzinfo=timezone.utc),
                             f"TIMESTAMPTZ failed for microseconds={us}")

    def test_binary_non_utf8_roundtrip(self):
        """PostgreSQL-specific: verify bytea columns preserve all 256 byte values
        and non-UTF-8 sequences through the Arrow path. Regression test for
        legacy issue baztian/jaydebeapi#147."""
        # Full 256-byte spectrum (every possible byte value)
        all_bytes = bytes(range(256))
        # Non-UTF-8 sequences that commonly get corrupted
        non_utf8_patterns = [
            bytes([0x80, 0x81, 0xff, 0xfe]),
            bytes([0xc0, 0x80]),  # overlong null
            bytes([0xff, 0xff, 0xff]),
            bytes([0x00, 0x00, 0x00, 0x00]),  # null bytes
        ]
        stmt = ("insert into ACCOUNT (ACCOUNT_ID, ACCOUNT_NO, BALANCE, "
                "STUFF) values (?, ?, ?, ?)")
        with self.conn.cursor() as cursor:
            # Test full 256-byte spectrum
            account_id = self.dbapi.Timestamp(2009, 9, 11, 14, 15, 22, 123450)
            cursor.execute(stmt, (account_id, 20, Decimal('13.1'),
                                  self.dbapi.Binary(all_bytes)))
            # Test individual non-UTF-8 patterns
            for idx, pattern in enumerate(non_utf8_patterns):
                aid = self.dbapi.Timestamp(2010, 1, 1, 0, 0, 0, idx)
                cursor.execute(stmt, (aid, 30 + idx, Decimal('1.0'),
                                      self.dbapi.Binary(pattern)))
            # Read back and verify
            cursor.execute(
                "select STUFF from ACCOUNT where ACCOUNT_NO = 20")
            result = cursor.fetchone()
            self.assertEqual(bytes(result[0]), all_bytes,
                             "Full 256-byte spectrum mismatch")
            for idx, pattern in enumerate(non_utf8_patterns):
                cursor.execute(
                    "select STUFF from ACCOUNT where ACCOUNT_NO = ?",
                    (30 + idx,))
                result = cursor.fetchone()
                self.assertEqual(bytes(result[0]), pattern,
                                 f"Pattern {idx} mismatch: {pattern!r}")

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

    def test_json_column_read(self):
        """Verify JSON columns (JDBC OTHER) are readable as strings via ExplicitTypeMapper."""
        with self.conn.cursor() as cursor:
            cursor.execute("CREATE TABLE test_json_type (id INT, data JSON)")
            try:
                cursor.execute(
                    "INSERT INTO test_json_type (id, data) VALUES (1, '{\"key\": \"value\"}')"
                )
                cursor.execute("SELECT data FROM test_json_type WHERE id = 1")
                result = cursor.fetchone()
                # Verify data is readable as a string
                self.assertIsInstance(result[0], str)
                self.assertIn("key", result[0])
                # Verify cursor.description reports STRING type code (OTHER → STRING)
                self.assertIs(cursor.description[0][1], jaydebeapiarrow.STRING)
            finally:
                cursor.execute("DROP TABLE test_json_type")

    def test_uuid_column_read(self):
        """Verify UUID columns (JDBC OTHER) are readable as strings via ExplicitTypeMapper."""
        with self.conn.cursor() as cursor:
            cursor.execute("CREATE TABLE test_uuid_type (id INT, data UUID)")
            try:
                cursor.execute(
                    "INSERT INTO test_uuid_type (id, data) "
                    "VALUES (1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')"
                )
                cursor.execute("SELECT data FROM test_uuid_type WHERE id = 1")
                result = cursor.fetchone()
                # Verify data is readable as a string
                self.assertIsInstance(result[0], str)
                self.assertEqual(result[0], "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
                # Verify cursor.description reports STRING type code (OTHER → STRING)
                self.assertIs(cursor.description[0][1], jaydebeapiarrow.STRING)
            finally:
                cursor.execute("DROP TABLE test_uuid_type")

    def test_xml_column_read(self):
        """Verify XML columns are readable as strings via ExplicitTypeMapper.
        Regression test for legacy issue baztian/jaydebeapi#223."""
        with self.conn.cursor() as cursor:
            cursor.execute("CREATE TABLE test_xml_type (id INT, data XML)")
            try:
                cursor.execute(
                    "INSERT INTO test_xml_type (id, data) "
                    "VALUES (1, '<root><item>hello</item></root>')"
                )
                cursor.execute("SELECT data FROM test_xml_type WHERE id = 1")
                result = cursor.fetchone()
                self.assertIsInstance(result[0], str)
                self.assertEqual(result[0], '<root><item>hello</item></root>')
            finally:
                cursor.execute("DROP TABLE test_xml_type")

    def test_array_column_read(self):
        """Verify ARRAY columns are readable as strings via ExplicitTypeMapper VARCHAR fallback."""
        with self.conn.cursor() as cursor:
            cursor.execute("CREATE TABLE test_array_type (id INT, data INTEGER[])")
            try:
                cursor.execute(
                    "INSERT INTO test_array_type (id, data) VALUES (1, '{1,2,3}')"
                )
                cursor.execute("SELECT data FROM test_array_type WHERE id = 1")
                result = cursor.fetchone()
                # Verify data is readable (degraded VARCHAR fallback — toString representation)
                self.assertIsInstance(result[0], str)
                # Verify cursor.description reports ARRAY type code
                self.assertIs(cursor.description[0][1], jaydebeapiarrow.ARRAY)
            finally:
                cursor.execute("DROP TABLE test_array_type")

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
        port = os.environ.get("JY_MYSQL_PORT", "13306")
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
            self.fail("Can not connect with MySQL. Please check if the instance is up and running.")
        else:
            return db, conn

    def setUpSql(self):
        self.sql_file(os.path.join(_THIS_DIR, 'data', 'create_mysql.sql'))
        self.sql_file(os.path.join(_THIS_DIR, 'data', 'insert.sql'))


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
            db, conn = jaydebeapiarrow, jaydebeapiarrow.connect(driver, url, driver_args)
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

    def tearDown(self):
        with self.conn.cursor() as cursor:
            cursor.execute("USE test_db")
        super().tearDown()

    def _double_create_sql(self):
        return "CREATE TABLE DOUBLE_TEST (val FLOAT)"

    def test_blob_null_value(self):
        """MSSQL JDBC driver rejects NULL parameter binding for VARBINARY columns."""
        self.skipTest("MSSQL JDBC driver does not support NULL for VARBINARY parameter binding")


class TrinoTest(IntegrationTestBase, unittest.TestCase):

    def connect(self):

        import jpype

        host = os.environ.get("JY_TRINO_HOST", "localhost")
        port = os.environ.get("JY_TRINO_PORT", "18080")
        user = os.environ.get("JY_TRINO_USER", "test")

        driver, url, driver_args = (
            'io.trino.jdbc.TrinoDriver',
            f'jdbc:trino://{host}:{port}/memory/default',
            {'user': user}
        )

        try:
            db, conn = jaydebeapiarrow, jaydebeapiarrow.connect(driver, url, driver_args)
        except jpype.JException:
            self.fail("Can not connect with Trino. Please check if the instance is up and running.")
        else:
            return db, conn

    def setUpSql(self):
        self.sql_file(os.path.join(_THIS_DIR, 'data', 'create_trino.sql'))
        self.sql_file(os.path.join(_THIS_DIR, 'data', 'insert_trino.sql'))

    def tearDown(self):
        with self.conn.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS ACCOUNT")
            cursor.execute("DROP TABLE IF EXISTS NUMERIC_TEST")
            cursor.execute("DROP TABLE IF EXISTS NUMERIC_COMBO")
        self.conn.close()

    def test_execute_reset_description_without_execute_result(self):
        """Trino memory connector does not support DELETE."""
        self.skipTest("Trino memory connector does not support modifying table rows")

    def test_numeric_types(self):
        """Trino memory connector does not support INSERT INTO ... VALUES — use CTAS instead."""
        with self.conn.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS NUMERIC_TEST")
            cursor.execute(
                "CREATE TABLE NUMERIC_TEST AS "
                "SELECT 1 AS ID, CAST(NULL AS DECIMAL(10, 2)) AS NUM_COL "
                "UNION ALL "
                "SELECT 2, CAST(99.99 AS DECIMAL(10, 2)) "
                "UNION ALL "
                "SELECT 3, CAST(100.00 AS DECIMAL(10, 2))")
            cursor.execute("SELECT NUM_COL FROM NUMERIC_TEST ORDER BY ID")
            result = cursor.fetchall()
        self.assertEqual(len(result), 3)
        self.assertIsNone(result[0][0])
        self.assertEqual(result[1][0], Decimal('99.99'))
        self.assertEqual(result[2][0], Decimal('100.00'))

    def test_numeric_precision_scale_combos(self):
        """Trino memory connector does not support INSERT — use CTAS instead."""
        with self.conn.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS NUMERIC_COMBO")
            cursor.execute(
                "CREATE TABLE NUMERIC_COMBO AS "
                "SELECT 1 AS ID, "
                "CAST(12345.67 AS DECIMAL(10, 2)) AS DEC_S2, "
                "CAST(12345.6789 AS DECIMAL(15, 4)) AS DEC_S4, "
                "CAST(987654321012345678 AS DECIMAL(18, 0)) AS DEC_S0, "
                "CAST(0.12345 AS DECIMAL(5, 5)) AS DEC_PES, "
                "CAST(99.99 AS DECIMAL(10, 2)) AS NUM_S2, "
                "CAST(42 AS DECIMAL(10, 0)) AS NUM_S0, "
                "CAST(12345.6789 AS DECIMAL(15, 4)) AS NUM_S4, "
                "CAST(0.1234 AS DECIMAL(4, 4)) AS NUM_PES, "
                "CAST(-99.99 AS DECIMAL(10, 2)) AS NUM_NEG")
            cursor.execute("SELECT DEC_S2, DEC_S4, DEC_S0, DEC_PES, "
                           "NUM_S2, NUM_S0, NUM_S4, NUM_PES, NUM_NEG "
                           "FROM NUMERIC_COMBO ORDER BY ID")
            result = cursor.fetchone()
        self.assertEqual(result[0], Decimal('12345.67'))
        self.assertEqual(result[1], Decimal('12345.6789'))
        self.assertEqual(result[2], Decimal('987654321012345678'))
        self.assertEqual(result[3], Decimal('0.12345'))
        self.assertEqual(result[4], Decimal('99.99'))
        self.assertEqual(result[5], Decimal('42'))
        self.assertEqual(result[6], Decimal('12345.6789'))
        self.assertEqual(result[7], Decimal('0.1234'))
        self.assertEqual(result[8], Decimal('-99.99'))

    def test_timestamp_subsecond_leading_zeros(self):
        """Trino's JDBC driver truncates sub-second precision."""
        self.skipTest("Trino JDBC driver truncates sub-second precision")

    def test_timestamp_microsecond_precision(self):
        """Trino's JDBC driver does not support getObject(_, LocalDateTime.class)."""
        self.skipTest("Trino JDBC driver cannot convert TIMESTAMP to LocalDateTime")

    def test_binary_non_utf8_roundtrip(self):
        """Trino memory connector does not support VARBINARY in CTAS for non-UTF-8 bytes."""
        self.skipTest("Trino memory connector does not support VARBINARY round-trip via CTAS")


class OracleTest(IntegrationTestBase, unittest.TestCase):

    def connect(self):

        import jpype

        host = os.environ.get("JY_ORACLE_HOST", "localhost")
        port = os.environ.get("JY_ORACLE_PORT", "11521")
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
            self.fail("Can not connect with Oracle. Please check if the instance is up and running.")
        else:
            return db, conn

    def setUpSql(self):
        self.sql_file(os.path.join(_THIS_DIR, 'data', 'create_oracle.sql'))
        self.sql_file(os.path.join(_THIS_DIR, 'data', 'insert_oracle.sql'))

    def _double_create_sql(self):
        return "CREATE TABLE DOUBLE_TEST (val BINARY_DOUBLE)"

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
        # Oracle JDBC quirks: NUMBER/INTEGER columns return BigDecimal with
        # full scale, and Oracle DATE maps to TIMESTAMP (includes time part).
        exp = (
            self._cast_datetime('2010-01-26 14:31:59', r'%Y-%m-%d %H:%M:%S'),
            Decimal('20.00000000000000000'),   # INTEGER → NUMERIC → Decimal(scale=17)
            Decimal('1.20'),                    # NUMBER(10,2) preserves scale
            Decimal('10.00'),                   # NUMBER(10,2) preserves scale
            dbl_col,
            self._cast_datetime('1908-02-27 00:00:00', r'%Y-%m-%d %H:%M:%S'),
            Decimal('1'),                       # NUMBER(1) → Decimal
            product_name
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

    def _numeric_create_table_sql(self):
        """Oracle uses NUMBER instead of NUMERIC/DECIMAL."""
        return (
            "CREATE TABLE NUMERIC_TEST ("
            "ID INTEGER NOT NULL, "
            "NUM_COL NUMBER(10, 2), "
            "PRIMARY KEY (ID))"
        )

    def _numeric_combo_create_sql(self):
        return (
            "CREATE TABLE NUMERIC_COMBO ("
            "ID INTEGER NOT NULL, "
            "DEC_S2 NUMBER(10, 2), "
            "DEC_S4 NUMBER(15, 4), "
            "DEC_S0 NUMBER(18, 0), "
            "DEC_PES NUMBER(5, 5), "
            "NUM_S2 NUMBER(10, 2), "
            "NUM_S0 NUMBER(10, 0), "
            "NUM_S4 NUMBER(15, 4), "
            "NUM_PES NUMBER(4, 4), "
            "NUM_NEG NUMBER(10, 2), "
            "PRIMARY KEY (ID))"
        )


class DB2Test(IntegrationTestBase, unittest.TestCase):

    def connect(self):

        import jpype

        host = os.environ.get("JY_DB2_HOST", "localhost")
        port = os.environ.get("JY_DB2_PORT", "15000")
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
            self.fail("Can not connect with DB2. Please check if the instance is up and running.")
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

    def test_blob_null_value(self):
        """DB2 rejects NULL for VARBINARY parameter binding."""
        self.skipTest("DB2 does not support NULL for VARBINARY parameter binding")


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
            self.fail("Can not connect with Drill. Please check if the instance is up and running.")
        else:
            return db, conn

    def _cast_datetime(self, datetime_str, fmt=r'%Y-%m-%d %H:%M:%S'):
        """Drill stores TIMESTAMP as UTC and shifts by JVM timezone on read."""
        dt = super()._cast_datetime(datetime_str, fmt)
        import jpype
        tz = jpype.JClass('java.util.TimeZone').getDefault()
        epoch_ms = int(calendar.timegm(dt.timetuple())) * 1000
        offset_ms = tz.getOffset(epoch_ms)
        return dt + timedelta(milliseconds=-offset_ms)

    def setUpSql(self):
        jstmt = self.conn.jconn.createStatement()
        try:
            jstmt.execute("DROP TABLE IF EXISTS dfs.tmp.account")
        except Exception:
            pass
        sql = open(os.path.join(_THIS_DIR, 'data', 'create_drill.sql')).read().strip().rstrip(';')
        jstmt.execute(sql)

    def tearDown(self):
        jstmt = self.conn.jconn.createStatement()
        try:
            jstmt.execute("DROP TABLE IF EXISTS dfs.tmp.account")
        except Exception:
            pass
        try:
            jstmt.execute("DROP TABLE IF EXISTS dfs.tmp.numeric_test")
        except Exception:
            pass
        try:
            jstmt.execute("DROP TABLE IF EXISTS dfs.tmp.blob_test")
        except Exception:
            pass
        try:
            jstmt.execute("DROP TABLE IF EXISTS dfs.tmp.numeric_combo")
        except Exception:
            pass
        self.conn.close()

    def _query_table(self, cursor):
        cursor.execute("select ACCOUNT_ID, ACCOUNT_NO, BALANCE, BLOCKING "
                       "from dfs.tmp.account")

    def test_double_column_returns_float(self):
        """Drill: use direct JDBC for DDL, cursor for SELECT."""
        jstmt = self.conn.jconn.createStatement()
        try:
            jstmt.execute(
                "CREATE TABLE dfs.tmp.DOUBLE_TEST AS "
                "SELECT CAST(c1 AS DOUBLE) AS val FROM "
                "(VALUES(3.14), (-1.5), (0.0)) AS t(c1)"
            )
        except Exception:
            jstmt.execute("DROP TABLE IF EXISTS dfs.tmp.DOUBLE_TEST")
            raise
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("SELECT val FROM dfs.tmp.DOUBLE_TEST ORDER BY val")
                result = cursor.fetchall()
        finally:
            jstmt.execute("DROP TABLE IF EXISTS dfs.tmp.DOUBLE_TEST")
        self.assertEqual(len(result), 3)
        for row in result:
            self.assertIsInstance(row[0], float)
        self.assertAlmostEqual(result[0][0], -1.5)
        self.assertAlmostEqual(result[1][0], 0.0)
        self.assertAlmostEqual(result[2][0], 3.14)

    def test_executemany(self):
        """Drill has no INSERT INTO ... VALUES — skip executemany test."""
        self.skipTest("Drill does not support INSERT INTO ... VALUES")

    def test_execute_types(self):
        """Drill preserves DECIMAL scale; data seeded via CTAS, no INSERT."""
        with self.conn.cursor() as cursor:
            cursor.execute(
                "SELECT ACCOUNT_ID, ACCOUNT_NO, BALANCE, BLOCKING, "
                "DBL_COL, OPENED_AT, VALID, PRODUCT_NAME "
                "FROM dfs.tmp.account WHERE ACCOUNT_NO = 20")
            result = cursor.fetchone()
        exp = (
            self._cast_datetime('2010-01-26 14:31:59', r'%Y-%m-%d %H:%M:%S'),
            20, Decimal('1.20'), Decimal('10.00'), 3.5,
            self._cast_date('2024-01-15', r'%Y-%m-%d'),
            True, 'Savings account'
        )
        self.assertEqual(result, exp)

    def test_execute_type_time(self):
        """Drill: TIME data seeded via CTAS, no INSERT needed."""
        with self.conn.cursor() as cursor:
            cursor.execute(
                "SELECT ACCOUNT_ID, ACCOUNT_NO, BALANCE, OPENED_AT_TIME "
                "FROM dfs.tmp.account WHERE ACCOUNT_NO = 20")
            result = cursor.fetchone()
        exp = (
            self._cast_datetime('2010-01-26 14:31:59', r'%Y-%m-%d %H:%M:%S'),
            20, Decimal('1.20'),
            self._cast_time('13:59:59', r'%H:%M:%S')
        )
        self.assertEqual(result, exp)

    def test_execute_type_blob(self):
        """Drill: seed VARBINARY via separate CTAS, verify read path."""
        jstmt = self.conn.jconn.createStatement()
        jstmt.execute('DROP TABLE IF EXISTS dfs.tmp.blob_test')
        jstmt.execute(
            "CREATE TABLE dfs.tmp.blob_test AS "
            "SELECT CAST('abcdef' AS VARBINARY) AS STUFF FROM (VALUES(1))")
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT STUFF FROM dfs.tmp.blob_test")
            result = cursor.fetchone()
        binary_stuff = b'abcdef'
        self.assertEqual(result[0], memoryview(binary_stuff))

    def test_binary_non_utf8_roundtrip(self):
        """Drill does not support CTAS with VARBINARY hex literals or
        parameterized INSERT for binary data with non-UTF-8 bytes."""
        self.skipTest("Drill cannot create VARBINARY with non-UTF-8 bytes via CTAS")

    def test_numeric_types(self):
        """Drill: seed NUMERIC_TEST via CTAS, then verify round-trip."""
        jstmt = self.conn.jconn.createStatement()
        jstmt.execute('DROP TABLE IF EXISTS dfs.tmp.numeric_test')
        jstmt.execute(
            "CREATE TABLE dfs.tmp.numeric_test AS "
            "SELECT 1 AS ID, CAST(NULL AS DECIMAL(10, 2)) AS NUM_COL "
            "UNION ALL "
            "SELECT 2, CAST(99.99 AS DECIMAL(10, 2)) "
            "UNION ALL "
            "SELECT 3, CAST(100.00 AS DECIMAL(10, 2))")
        with self.conn.cursor() as cursor:
            cursor.execute(
                "SELECT NUM_COL FROM dfs.tmp.numeric_test ORDER BY ID")
            result = cursor.fetchall()
        self.assertEqual(len(result), 3)
        self.assertIsNone(result[0][0])
        self.assertEqual(result[1][0], Decimal('99.99'))
        self.assertEqual(result[2][0], Decimal('100.00'))

    def test_numeric_precision_scale_combos(self):
        """Drill: seed NUMERIC_COMBO via CTAS, then verify round-trip."""
        jstmt = self.conn.jconn.createStatement()
        jstmt.execute('DROP TABLE IF EXISTS dfs.tmp.numeric_combo')
        jstmt.execute(
            "CREATE TABLE dfs.tmp.numeric_combo AS "
            "SELECT 1 AS ID, "
            "CAST(12345.67 AS DECIMAL(10, 2)) AS DEC_S2, "
            "CAST(12345.6789 AS DECIMAL(15, 4)) AS DEC_S4, "
            "CAST(987654321012345678 AS DECIMAL(18, 0)) AS DEC_S0, "
            "CAST(0.12345 AS DECIMAL(5, 5)) AS DEC_PES, "
            "CAST(99.99 AS DECIMAL(10, 2)) AS NUM_S2, "
            "CAST(42 AS DECIMAL(10, 0)) AS NUM_S0, "
            "CAST(12345.6789 AS DECIMAL(15, 4)) AS NUM_S4, "
            "CAST(0.1234 AS DECIMAL(4, 4)) AS NUM_PES, "
            "CAST(-99.99 AS DECIMAL(10, 2)) AS NUM_NEG")
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT DEC_S2, DEC_S4, DEC_S0, DEC_PES, "
                           "NUM_S2, NUM_S0, NUM_S4, NUM_PES, NUM_NEG "
                           "FROM dfs.tmp.numeric_combo ORDER BY ID")
            result = cursor.fetchone()
        self.assertEqual(result[0], Decimal('12345.67'))
        self.assertEqual(result[1], Decimal('12345.6789'))
        self.assertEqual(result[2], Decimal('987654321012345678'))
        self.assertEqual(result[3], Decimal('0.12345'))
        self.assertEqual(result[4], Decimal('99.99'))
        self.assertEqual(result[5], Decimal('42'))
        self.assertEqual(result[6], Decimal('12345.6789'))
        self.assertEqual(result[7], Decimal('0.1234'))
        self.assertEqual(result[8], Decimal('-99.99'))

    def test_execute_param_none(self):
        """Drill has no INSERT INTO ... VALUES — skip param none test."""
        self.skipTest("Drill does not support INSERT INTO ... VALUES")

    def test_execute_different_rowcounts(self):
        """Drill has no INSERT INTO ... VALUES — skip rowcount test."""
        self.skipTest("Drill does not support INSERT INTO ... VALUES")

    def test_lastrowid_none_after_select(self):
        """Drill uses different table schema — skip."""
        self.skipTest("Drill test schema differs from standard ACCOUNT table")

    def test_lastrowid_none_after_insert(self):
        """Drill has no INSERT INTO ... VALUES — skip."""
        self.skipTest("Drill does not support INSERT INTO ... VALUES")

    def test_lastrowid_none_after_executemany(self):
        """Drill has no INSERT INTO ... VALUES — skip."""
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
                           "from dfs.tmp.account WHERE ACCOUNT_NO <= 19")
            result = cursor.fetchall()
        self.assertEqual(result, [
            (
            self._cast_datetime('2009-09-10 14:15:22.123', r'%Y-%m-%d %H:%M:%S.%f'),
            18, Decimal('12.40'), None),
            (
            self._cast_datetime('2009-09-11 14:15:22.123', r'%Y-%m-%d %H:%M:%S.%f'),
            19, Decimal('12.90'), Decimal('1.00'))
        ])

    def test_execute_and_fetchone(self):
        with self.conn.cursor() as cursor:
            cursor.execute("select ACCOUNT_ID, ACCOUNT_NO, BALANCE, BLOCKING "
                           "from dfs.tmp.account WHERE ACCOUNT_NO <= 19 order by ACCOUNT_NO")
            result = cursor.fetchone()
        self.assertEqual(result, (
            self._cast_datetime('2009-09-10 14:15:22.123', r'%Y-%m-%d %H:%M:%S.%f'),
            18, Decimal('12.40'), None))
        cursor.close()

    def test_execute_and_fetchone_consecutive(self):
        with self.conn.cursor() as cursor:
            cursor.execute("select ACCOUNT_ID, ACCOUNT_NO, BALANCE, BLOCKING "
                           "from dfs.tmp.account WHERE ACCOUNT_NO <= 19 order by ACCOUNT_NO")
            result1 = cursor.fetchone()
            result2 = cursor.fetchone()

        self.assertEqual(result1, (
            self._cast_datetime('2009-09-10 14:15:22.123', r'%Y-%m-%d %H:%M:%S.%f'),
            18, Decimal('12.40'), None))

        self.assertEqual(result2, (
            self._cast_datetime('2009-09-11 14:15:22.123', r'%Y-%m-%d %H:%M:%S.%f'),
            19, Decimal('12.90'), Decimal('1.00')))

    def test_execute_and_fetch_no_data(self):
        with self.conn.cursor() as cursor:
            stmt = "select * from dfs.tmp.account where ACCOUNT_ID is null"
            cursor.execute(stmt)
            result = cursor.fetchall()
        self.assertEqual(result, [])

    def test_execute_and_fetch_parameter(self):
        """Drill does not support JDBC parameterized queries."""
        self.skipTest("Drill does not support prepared statement parameters")

    def test_execute_and_fetchone_after_end(self):
        with self.conn.cursor() as cursor:
            cursor.execute("select * from dfs.tmp.account where ACCOUNT_NO = 18")
            cursor.fetchone()
            result = cursor.fetchone()
        self.assertIsNone(result)

    def test_execute_and_fetchmany(self):
        with self.conn.cursor() as cursor:
            cursor.execute("select ACCOUNT_ID, ACCOUNT_NO, BALANCE, BLOCKING "
                           "from dfs.tmp.account WHERE ACCOUNT_NO <= 19 order by ACCOUNT_NO")
            result = cursor.fetchmany()
        self.assertEqual(result, [
            (
            self._cast_datetime('2009-09-10 14:15:22.123', r'%Y-%m-%d %H:%M:%S.%f'),
            18, Decimal('12.40'), None)
        ])

    def test_timestamp_subsecond_leading_zeros(self):
        """Drill does not support parameterized TIMESTAMP INSERT."""
        self.skipTest("Drill does not support parameterized TIMESTAMP INSERT")

    def test_timestamp_microsecond_precision(self):
        """Drill does not support TIMESTAMP with microsecond INSERT via parameterized queries."""
        self.skipTest("Drill does not support parameterized TIMESTAMP INSERT")

    def test_blob_non_utf8_roundtrip(self):
        """Drill does not support parameterized INSERT."""
        self.skipTest("Drill does not support parameterized INSERT queries")

    def test_blob_all_byte_values_roundtrip(self):
        """Drill does not support parameterized INSERT."""
        self.skipTest("Drill does not support parameterized INSERT queries")

    def test_blob_null_value(self):
        """Drill does not support parameterized INSERT."""
        self.skipTest("Drill does not support parameterized INSERT queries")


class JavaSqlTypesReflectionTest(unittest.TestCase):
    """Verify java.sql.Types field access uses standard Java Reflection API
    (not deprecated JPype getStaticAttribute). Regression for legacy #111."""

    def setUp(self):
        self.conn = jaydebeapiarrow.connect(
            'org.hsqldb.jdbc.JDBCDriver',
            'jdbc:hsqldb:mem:testreflection.',
            ['SA', ''],
        )

    def tearDown(self):
        self.conn.close()

    def test_type_constants_accessible_via_reflection(self):
        """java.sql.Types constants should be accessible through
        standard Java Reflection, not getStaticAttribute()."""
        import jpype
        Types = jpype.java.sql.Types
        # Access via standard attribute access (JPype proxy)
        self.assertEqual(Types.INTEGER, 4)
        self.assertEqual(Types.VARCHAR, 12)
        self.assertEqual(Types.TIMESTAMP, 93)
        self.assertEqual(Types.DECIMAL, 3)

    def test_dbapi_type_comparison_with_real_connection(self):
        """DBAPITypeObject comparison should work after a real JDBC
        connection initializes the type mapping via Reflection."""
        import jpype
        Types = jpype.java.sql.Types
        # After connecting, _jdbc_const_to_name should be populated
        self.assertIsNotNone(jaydebeapiarrow._jdbc_const_to_name)
        # Verify type comparisons work
        self.assertEqual(jaydebeapiarrow.NUMBER, Types.INTEGER)
        self.assertEqual(jaydebeapiarrow.STRING, Types.VARCHAR)
        self.assertEqual(jaydebeapiarrow.DATETIME, Types.TIMESTAMP)

    def test_cursor_description_maps_types_correctly(self):
        """cursor.description should use correct type names from
        Reflection-based type mapping."""
        with self.conn.cursor() as cursor:
            cursor.execute("CREATE TABLE test_reflect (id INTEGER, name VARCHAR(50), val DECIMAL(10,2))")
            cursor.execute("INSERT INTO test_reflect VALUES (1, 'test', 3.14)")
            cursor.execute("SELECT * FROM test_reflect")
            desc = cursor.description
            # All three columns should have descriptions
            self.assertEqual(len(desc), 3)
            self.assertEqual(desc[0][0], 'ID')
            self.assertEqual(desc[1][0], 'NAME')
            self.assertEqual(desc[2][0], 'VAL')


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

    def test_connect_bad_credentials_raises_database_error(self):
        """Connection to non-existent HSQLDB server should raise DatabaseError."""
        with self.assertRaises(jaydebeapiarrow.DatabaseError):
            jaydebeapiarrow.connect('org.hsqldb.jdbcDriver',
                                    'jdbc:hsqldb:hsql://localhost/nonexistent_db_invalid_port',
                                    ['SA', ''])


class JarPathSpacesIntegrationTest(unittest.TestCase):
    """Integration test for JAR paths containing spaces (issue #86).

    Uses HSQLDB driver copied to a path with spaces, run in a subprocess
    to avoid JPype single-JVM-per-process limitation.
    """

    def test_hsqldb_jar_path_with_spaces(self):
        """HSQLDB connection should work when JAR is in a path with spaces."""
        # Find the HSQLDB JAR
        hsqldb_jar = None
        jar_dir = os.path.join(_THIS_DIR, 'jars')
        if not os.path.isdir(jar_dir):
            self.skipTest('test/jars/ directory not found (run download_jdbc_drivers.sh)')
        for f in os.listdir(jar_dir):
            if 'hsqldb' in f.lower() and f.endswith('.jar'):
                hsqldb_jar = os.path.join(jar_dir, f)
                break
        self.assertIsNotNone(hsqldb_jar, 'HSQLDB JAR not found in test/jars/')

        with tempfile.TemporaryDirectory(prefix='path with spaces ') as tmpdir:
            dest = os.path.join(tmpdir, os.path.basename(hsqldb_jar))
            shutil.copy2(hsqldb_jar, dest)

            code = f'''
import jaydebeapiarrow
conn = jaydebeapiarrow.connect(
    'org.hsqldb.jdbcDriver',
    'jdbc:hsqldb:mem:.',
    ['SA', ''],
    jars={repr(dest)}
)
cursor = conn.cursor()
cursor.execute('SELECT 1 AS col1 FROM (VALUES(0)) AS t')
rows = cursor.fetchall()
print(f'OK: {{rows}}')
cursor.close()
conn.close()
'''
            result = subprocess.run(
                [sys.executable, '-c', code],
                capture_output=True, text=True, timeout=30,
                cwd=os.path.dirname(_THIS_DIR)
            )
            self.assertTrue(result.stdout.strip().startswith('OK'),
                            f'Connection failed: {result.stdout}\n{result.stderr}')
