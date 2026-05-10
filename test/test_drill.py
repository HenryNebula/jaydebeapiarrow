#-*- coding: utf-8 -*-

import jaydebeapiarrow
import calendar
import os
import unittest

from decimal import Decimal
from datetime import datetime, timedelta
try:
    from test._base import IntegrationTestBase, _THIS_DIR
except ImportError:
    from _base import IntegrationTestBase, _THIS_DIR


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
            db, conn = jaydebeapiarrow, self._quiet_connect(
                driver, url, driver_args)
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

    def test_fetchone_after_ddl_returns_none(self):
        """Drill wraps SQL in SELECT — DDL must go through jconn.createStatement()."""
        self.skipTest("Drill cannot execute DDL through prepared statements")

    def test_fetchall_after_ddl_returns_empty(self):
        self.skipTest("Drill cannot execute DDL through prepared statements")

    def test_fetchmany_after_ddl_returns_empty(self):
        self.skipTest("Drill cannot execute DDL through prepared statements")

    def test_description_after_ddl_is_none(self):
        self.skipTest("Drill cannot execute DDL through prepared statements")

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

    def test_varchar_non_ascii_roundtrip(self):
        """Drill does not support parameterized INSERT."""
        self.skipTest("Drill does not support parameterized INSERT queries")

    def test_execute_param_datetime(self):
        """Drill does not support parameterized INSERT."""
        self.skipTest("Drill does not support parameterized INSERT queries")

    def test_timestamp_utc_roundtrip_no_timezone_shift(self):
        """Drill does not support parameterized INSERT."""
        self.skipTest("Drill does not support parameterized INSERT queries")

    def test_varchar_columns_return_data(self):
        """Drill does not support INSERT INTO ... VALUES."""
        self.skipTest("Drill does not support INSERT INTO ... VALUES")

    def test_iterator_closed_after_fetchall(self):
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT * FROM dfs.tmp.account")
            cursor.fetchall()
            self.assertIsNone(cursor._iter)

    def test_iterator_closed_after_fetchone_exhaustion(self):
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM dfs.tmp.account")
            cursor.fetchone()
            result = cursor.fetchone()
            self.assertIsNone(result)
            self.assertIsNone(cursor._iter)

    def test_iterator_closed_after_fetchmany_exhaustion(self):
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT * FROM dfs.tmp.account")
            cursor.fetchmany(size=1000)
            self.assertIsNone(cursor._iter)

    def test_repeated_query_cycles_release_resources(self):
        with self.conn.cursor() as cursor:
            for _ in range(5):
                cursor.execute("SELECT * FROM dfs.tmp.account")
                result = cursor.fetchall()
                self.assertTrue(len(result) > 0)
                self.assertIsNone(cursor._iter)
                self.assertEqual(cursor._buffer, [])

    def test_long_query_string_18k_characters(self):
        long_query = ("SELECT ACCOUNT_NO FROM dfs.tmp.account WHERE ACCOUNT_NO IN ("
                      + ",".join(str(i) for i in range(5000)) + ")")
        self.assertGreater(len(long_query), 18000)
        with self.conn.cursor() as cursor:
            cursor.execute(long_query)
            result = cursor.fetchall()
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 3)
        returned_ids = sorted(row[0] for row in result)
        self.assertEqual(returned_ids, [18, 19, 20])

    def test_description_returns_column_alias(self):
        self.skipTest("Drill does not support quoted identifiers")

    def test_lastrowid_populated_for_identity_column(self):
        self.skipTest("Drill does not support identity/auto-increment columns")
