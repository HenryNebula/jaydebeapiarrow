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

import jaydebeapiarrow
import os
import unittest

from decimal import Decimal
from datetime import datetime

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))

_SUPPRESS_LOGGING_ARGS = [
    '-Dorg.slf4j.simpleLogger.defaultLogLevel=off',
    '-Djava.util.logging.config.file=%s' % os.path.join(
        os.path.dirname(jaydebeapiarrow.__file__), 'logging.properties'),
]


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
        self._cleanup_tables()
        self.setUpSql()

    def test_connect_url_must_be_string(self):
        """Passing a list as url should raise ProgrammingError (issue #95)."""
        with self.assertRaises(jaydebeapiarrow.ProgrammingError) as ctx:
            jaydebeapiarrow.connect(
                'org.hsqldb.jdbcDriver',
                ['jdbc:hsqldb:mem:.', 'SA', '']
            )
        self.assertIn('url', str(ctx.exception).lower())

    def _cleanup_tables(self):
        """Drop any leftover tables from a previous failed test run."""
        with self.conn.cursor() as cursor:
            for table in ('ACCOUNT', 'NUMERIC_TEST', 'NUMERIC_COMBO',
                          'DOUBLE_TEST', 'BIGINT_TEST'):
                try:
                    cursor.execute(f"DROP TABLE {table}")
                except Exception:
                    pass

    @staticmethod
    def _quiet_connect(*args, **kwargs):
        """Wrapper around jaydebeapiarrow.connect() that silences Java
        loggers (slf4j-simple and java.util.logging) on the first call."""
        kwargs.setdefault('jvm_args', _SUPPRESS_LOGGING_ARGS)
        return jaydebeapiarrow.connect(*args, **kwargs)

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
        try:
            self.conn.jconn.setAutoCommit(True)
        except Exception:
            pass
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
        """lastrowid should be None after INSERT on a table without auto-generated keys.

        Per JDBC spec (Statement.getGeneratedKeys javadoc): "If this Statement
        object did not generate any keys, an empty ResultSet object is returned."
        ACCOUNT.ACCOUNT_ID is not AUTO_INCREMENT/IDENTITY, so no keys are generated.
        Drivers with implicit rowid (e.g. SQLite) should override this test.
        """
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

    def _autoincrement_create_sql(self):
        """DDL for a table with an auto-increment/identity column.
        Override for databases with different syntax.
        """
        return ("CREATE TABLE LASTROWID_TEST "
                "(id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, "
                "val VARCHAR(50))")

    def test_lastrowid_populated_for_identity_column(self):
        """lastrowid should return the generated key after INSERT on a table
        with an identity/auto-increment column.

        Per JDBC spec, getGeneratedKeys() returns a ResultSet with the
        auto-generated key when Statement.RETURN_GENERATED_KEYS was used.
        """
        with self.conn.cursor() as cursor:
            try:
                cursor.execute("DROP TABLE IF EXISTS LASTROWID_TEST")
            except Exception:
                try:
                    cursor.execute("DROP TABLE LASTROWID_TEST")
                except Exception:
                    pass
            cursor.execute(self._autoincrement_create_sql())
            try:
                cursor.execute("INSERT INTO LASTROWID_TEST (val) VALUES ('test')")
                self.assertIsNotNone(cursor.lastrowid)
                self.assertIsInstance(cursor.lastrowid, int)
            finally:
                try:
                    cursor.execute("DROP TABLE LASTROWID_TEST")
                except Exception:
                    pass

    def test_sql_exception_message_is_clean(self):
        """SQL exceptions should produce clean messages without JPype artefacts."""
        with self.conn.cursor() as cursor:
            with self.assertRaises(jaydebeapiarrow.DatabaseError) as cm:
                cursor.execute("SELECT * FROM nonexistent_table")
        msg = str(cm.exception)
        self.assertTrue("Exception" in msg, f"Expected 'Exception' in: {msg}")
        self.assertNotIn("java.sql.java.sql", msg)
        self.assertNotIn("com.microsoft.com.microsoft", msg)
        self.assertNotIn("oracle.jdbc.oracle.jdbc", msg)

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

    def test_description_returns_column_alias(self):
        """cursor.description should return the AS alias, not the table column name."""
        with self.conn.cursor() as cursor:
            cursor.execute('SELECT ACCOUNT_NO AS "ACCT_NUM" FROM ACCOUNT')
            self.assertEqual(cursor.description[0][0], "ACCT_NUM")

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

    def test_execute_param_datetime(self):
        """Verify Python datetime objects round-trip correctly via parameter binding."""
        stmt = ("insert into ACCOUNT "
                "(ACCOUNT_ID, ACCOUNT_NO, BALANCE, OPENED_AT, OPENED_AT_TIME) "
                "values (?, ?, ?, ?, ?)")
        ts = datetime(2024, 6, 15, 10, 30, 45, 123456)
        d = datetime(2024, 6, 15).date()
        t = datetime(2024, 6, 15, 10, 30, 45).time()
        with self.conn.cursor() as cursor:
            cursor.execute(stmt, (ts, 40, Decimal('7.0'), d, t))
            cursor.execute(
                "select ACCOUNT_ID, OPENED_AT, OPENED_AT_TIME "
                "from ACCOUNT where ACCOUNT_NO = 40")
            result = cursor.fetchone()
        # Timestamp: must match at least to second precision.
        # Some drivers (Trino) truncate to milliseconds; Oracle may drop
        # fractional seconds.  Compare the floor to whole seconds.
        self.assertEqual(result[0].replace(microsecond=0),
                         datetime(2024, 6, 15, 10, 30, 45))
        # Date: some drivers (Oracle) return datetime(2024,6,15,0,0) for
        # DATE columns; accept both forms.
        actual_date = result[1]
        if isinstance(actual_date, datetime):
            actual_date = actual_date.replace(hour=0, minute=0, second=0,
                                              microsecond=0)
            self.assertEqual(actual_date, datetime(2024, 6, 15))
        else:
            self.assertEqual(actual_date, datetime(2024, 6, 15).date())
        # Time: some drivers (Oracle) return datetime(1970,1,1,HH,MM,SS)
        # instead of a pure time object; accept both forms.
        actual_time = result[2]
        if isinstance(actual_time, datetime):
            self.assertEqual(actual_time.hour, 10)
            self.assertEqual(actual_time.minute, 30)
            self.assertEqual(actual_time.second, 45)
        else:
            self.assertEqual(actual_time.replace(microsecond=0),
                             datetime(2024, 6, 15, 10, 30, 45).time())

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
            cursor.execute("SELECT * FROM ACCOUNT")
            cursor.fetchall()
            self.assertIsNone(cursor._iter)

    def test_iterator_closed_after_fetchone_exhaustion(self):
        """After fetchone exhausts the result set, iterator should be closed."""
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM ACCOUNT")
            cursor.fetchone()
            result = cursor.fetchone()
            self.assertIsNone(result)
            self.assertIsNone(cursor._iter)

    def test_iterator_closed_after_fetchmany_exhaustion(self):
        """After fetchmany exhausts the result set, iterator should be closed."""
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT * FROM ACCOUNT")
            cursor.fetchmany(size=1000)
            self.assertIsNone(cursor._iter)

    def test_repeated_query_cycles_release_resources(self):
        """Repeated execute/fetchall cycles should not accumulate iterators
        or buffers (memory leak regression, legacy #227)."""
        with self.conn.cursor() as cursor:
            for _ in range(5):
                cursor.execute("SELECT * FROM ACCOUNT")
                result = cursor.fetchall()
                self.assertTrue(len(result) > 0)
                self.assertIsNone(cursor._iter)
                self.assertEqual(cursor._buffer, [])

    def test_timestamp_utc_roundtrip_no_timezone_shift(self):
        """Verify TIMESTAMP values round-trip without timezone shifting.

        Regression test for baztian/jaydebeapi#73. Legacy jaydebeapi returned
        timestamps in the JVM's local timezone instead of UTC. This test
        inserts specific timestamp values via parameter binding and verifies
        they are returned as naive datetime objects with exact values — no
        timezone offset applied.
        """
        test_cases = [
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
            cursor.execute(
                "SELECT ACCOUNT_NO, BALANCE, PRODUCT_NAME "
                "FROM ACCOUNT WHERE ACCOUNT_NO >= 100 ORDER BY ACCOUNT_NO"
            )
            result = cursor.fetchall()
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0][0], 100)
        self.assertEqual(result[0][1], Decimal('99.99'))
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

    def _safe_drop(self, cursor, table):
        """Drop a table, ignoring errors if it doesn't exist."""
        try:
            cursor.execute(f"DROP TABLE {table}")
        except Exception:
            pass

    def test_fetchone_after_ddl_returns_none(self):
        """fetchone() after a DDL statement (CREATE TABLE) should return None."""
        with self.conn.cursor() as cursor:
            self._safe_drop(cursor, "ddl_test")
            cursor.execute("CREATE TABLE ddl_test (id INTEGER)")
            result = cursor.fetchone()
        self.assertIsNone(result)

    def test_fetchall_after_ddl_returns_empty(self):
        """fetchall() after a DDL statement (CREATE TABLE) should return []."""
        with self.conn.cursor() as cursor:
            self._safe_drop(cursor, "ddl_test2")
            cursor.execute("CREATE TABLE ddl_test2 (id INTEGER)")
            result = cursor.fetchall()
        self.assertEqual(result, [])

    def test_fetchmany_after_ddl_returns_empty(self):
        """fetchmany() after a DDL statement (CREATE TABLE) should return []."""
        with self.conn.cursor() as cursor:
            self._safe_drop(cursor, "ddl_test3")
            cursor.execute("CREATE TABLE ddl_test3 (id INTEGER)")
            result = cursor.fetchmany(5)
        self.assertEqual(result, [])

    def test_description_after_ddl_is_none(self):
        """cursor.description should be None after a DDL statement."""
        with self.conn.cursor() as cursor:
            self._safe_drop(cursor, "ddl_test4")
            cursor.execute("CREATE TABLE ddl_test4 (id INTEGER)")
        self.assertIsNone(cursor.description)


class SqliteTestBase(IntegrationTestBase):

    def setUpSql(self):
        self.sql_file(os.path.join(_THIS_DIR, 'data', 'create.sql'))
        self.sql_file(os.path.join(_THIS_DIR, 'data', 'insert.sql'))

    def _autoincrement_create_sql(self):
        return ("CREATE TABLE LASTROWID_TEST "
                "(id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "val VARCHAR(50))")

    def test_execute_param_datetime(self):
        """SQLite JDBC does not support binding datetime.time parameters."""
        self.skipTest("SQLite JDBC does not support datetime.time parameter binding")
