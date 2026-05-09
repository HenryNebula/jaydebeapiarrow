#-*- coding: utf-8 -*-

import jaydebeapiarrow
import os
import unittest

from decimal import Decimal
from datetime import datetime, timezone
try:
    from test._base import IntegrationTestBase, _THIS_DIR, _SUPPRESS_LOGGING_ARGS
except ImportError:
    from _base import IntegrationTestBase, _THIS_DIR, _SUPPRESS_LOGGING_ARGS


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
            db, conn = jaydebeapiarrow, jaydebeapiarrow.connect(
                driver, url, driver_args,
                experimental={'jvm_args': _SUPPRESS_LOGGING_ARGS})
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
        """Verify ARRAY columns are readable as native Python lists via C Data Interface."""
        with self.conn.cursor() as cursor:
            cursor.execute(
                "CREATE TABLE test_array_type ("
                "  id INT, "
                "  int_vals INTEGER[], "
                "  str_vals TEXT[], "
                "  bool_vals BOOLEAN[], "
                "  float_vals DOUBLE PRECISION[])")
            try:
                cursor.execute(
                    "INSERT INTO test_array_type VALUES ("
                    "  1, "
                    "  ARRAY[1, 2, 3], "
                    "  ARRAY['foo', 'bar'], "
                    "  ARRAY[TRUE, FALSE], "
                    "  ARRAY[1.5, 2.5])")

                # Integer array
                cursor.execute("SELECT int_vals FROM test_array_type WHERE id = 1")
                result = cursor.fetchone()
                self.assertIsInstance(result[0], list)
                self.assertEqual(result[0], [1, 2, 3])
                self.assertIs(cursor.description[0][1], jaydebeapiarrow.ARRAY)

                # Text array
                cursor.execute("SELECT str_vals FROM test_array_type WHERE id = 1")
                result = cursor.fetchone()
                self.assertIsInstance(result[0], list)
                self.assertEqual(result[0], ["foo", "bar"])

                # Boolean array
                cursor.execute("SELECT bool_vals FROM test_array_type WHERE id = 1")
                result = cursor.fetchone()
                self.assertIsInstance(result[0], list)
                self.assertEqual(result[0], [True, False])

                # Float array
                cursor.execute("SELECT float_vals FROM test_array_type WHERE id = 1")
                result = cursor.fetchone()
                self.assertIsInstance(result[0], list)
                self.assertEqual(result[0], [1.5, 2.5])

                # Multiple array columns in one row
                cursor.execute("SELECT int_vals, str_vals FROM test_array_type WHERE id = 1")
                result = cursor.fetchone()
                self.assertEqual(result[0], [1, 2, 3])
                self.assertEqual(result[1], ["foo", "bar"])
            finally:
                cursor.execute("DROP TABLE test_array_type")

    def test_array_parameter_binding(self):
        """Python list parameters should be bindable as SQL ARRAYs."""
        with self.conn.cursor() as cursor:
            cursor.execute("CREATE TABLE test_array_bind (id INT, data INTEGER[])")
            try:
                cursor.execute(
                    "INSERT INTO test_array_bind (id, data) VALUES (?, ?)",
                    (1, [10, 20, 30]))
                cursor.execute("SELECT data FROM test_array_bind WHERE id = 1")
                result = cursor.fetchone()
                self.assertEqual(result[0], [10, 20, 30])
            finally:
                cursor.execute("DROP TABLE test_array_bind")

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
