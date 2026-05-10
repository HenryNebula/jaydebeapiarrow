#-*- coding: utf-8 -*-

import jaydebeapiarrow
import os
import unittest

from decimal import Decimal
try:
    from test._base import IntegrationTestBase, _THIS_DIR
except ImportError:
    from _base import IntegrationTestBase, _THIS_DIR


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
            db, conn = jaydebeapiarrow, self._quiet_connect(
                driver, url, driver_args)
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

    def test_varchar_non_ascii_roundtrip(self):
        """Trino memory connector does not support INSERT INTO ... VALUES."""
        self.skipTest("Trino memory connector does not support INSERT INTO ... VALUES")

    def test_timestamp_utc_roundtrip_no_timezone_shift(self):
        """Trino memory connector does not support INSERT INTO ... VALUES."""
        self.skipTest("Trino memory connector does not support INSERT INTO ... VALUES")

    def test_varchar_columns_return_data(self):
        """Trino memory connector does not support INSERT INTO ... VALUES."""
        self.skipTest("Trino memory connector does not support INSERT INTO ... VALUES")

    def test_commit_with_autocommit_disabled(self):
        self.skipTest("Trino memory connector does not support transactions")

    def test_commit_with_autocommit_enabled(self):
        self.skipTest("Trino memory connector does not support transactions")

    def test_rollback_with_autocommit_disabled(self):
        self.skipTest("Trino memory connector does not support transactions")

    def test_rollback_with_autocommit_enabled(self):
        self.skipTest("Trino memory connector does not support transactions")

    def test_lastrowid_populated_for_identity_column(self):
        self.skipTest("Trino does not support identity/auto-increment columns")
