#-*- coding: utf-8 -*-

# Copyright 2015 Bastian Bowe
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
from datetime import datetime, timedelta
from decimal import Decimal

try:
    import unittest2 as unittest
except ImportError:
    import unittest

class MockTest(unittest.TestCase):

    def setUp(self):
        self.conn = jaydebeapiarrow.connect('org.jaydebeapi.mockdriver.MockDriver',
                                       'jdbc:jaydebeapi://dummyurl')

    def tearDown(self):
        self.conn.close()

    # JDBC types not supported by the Arrow data path (no Arrow type mapping)
    _ARROW_UNSUPPORTED_TYPES = {'OTHER', 'NCLOB', 'SQLXML', 'ROWID', 'ARRAY',
                                'TIME_WITH_TIMEZONE', 'TIMESTAMP_WITH_TIMEZONE'}

    def test_all_db_api_type_objects_have_valid_mapping(self):
        extra_type_mappings = {
            'DATE': 'getDate',
            'TIME': 'getTime',
            'TIMESTAMP': 'getTimestamp',
            'STRING': 'getString',
            'TEXT': 'getString',
            'BINARY': 'getBinary',
            'NUMBER': 'getInt',
            'FLOAT': 'getDouble',
            'DECIMAL': 'getBigDecimal',
            'ROWID': 'getRowID'
        }
        for db_api_type in jaydebeapiarrow.__dict__.values():
            if isinstance(db_api_type, jaydebeapiarrow.DBAPITypeObject):
                for jsql_type_name in db_api_type.values:
                    if jsql_type_name in self._ARROW_UNSUPPORTED_TYPES:
                        continue
                    self.conn.jconn.mockType(jsql_type_name)
                    with self.conn.cursor() as cursor:
                        cursor.execute("dummy stmt")
                        cursor.fetchone()
                    # verify = self.conn.jconn.verifyResultSet()
                    # verify_get = getattr(verify,
                    #                      extra_type_mappings.get(db_api_type.group_name,
                    #                                              'getObject'))
                    # verify_get(1)

    def test_ancient_date_mapped(self):
        date = datetime(year=70, month=1, day=1).date()
        self.conn.jconn.mockDateResult(date.year, date.month, date.day)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0], date)

    def test_decimal_scale_zero(self):
        self.conn.jconn.mockBigDecimalResult(12345, 0)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0], Decimal("12345"))

    def test_decimal_places(self):
        self.conn.jconn.mockBigDecimalResult(12345, 1)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0], Decimal("1234.5"))

    def test_double_decimal(self):
        self.conn.jconn.mockDoubleDecimalResult(1234.5)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0], Decimal("1234.5"))

    def test_decimal_null_value(self):
        """SQL NULL in a DECIMAL column should return None, not crash or return 0."""
        self.conn.jconn.mockNullDecimalResult(10, 2)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertIsNone(result[0])

    def test_decimal_high_precision_overflow(self):
        """BigDecimal with scale > vector scale is safely rounded with HALF_UP
        when the data exceeds the vector's configured scale."""
        import jpype
        BigDecimal = jpype.JClass("java.math.BigDecimal")
        # Value has scale 20, but vector is configured with scale 2.
        # HALF_UP rounds to 2 decimal places.
        value = BigDecimal("123456789012345678.12345678901234567890")
        self.conn.jconn.mockHighPrecisionDecimalResult(value, 38, 2)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0], Decimal("123456789012345678.12"))

    def test_decimal_true_precision_overflow_has_actionable_error(self):
        """Values that cannot fit Arrow DECIMAL precision should fail loudly
        instead of being returned as NULL."""
        import jpype
        BigDecimal = jpype.JClass("java.math.BigDecimal")
        value = BigDecimal("123456789012345678901234567890123456789")
        self.conn.jconn.mockHighPrecisionDecimalResult(value, 38, 0)

        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            with self.assertRaises(Exception) as cm:
                cursor.fetchone()

        message = str(cm.exception)
        self.assertIn("Could not convert DECIMAL/NUMERIC value", message)
        self.assertIn("Arrow DECIMAL(38, 0)", message)
        self.assertIn("Cast the column in SQL", message)
        self.assertIn("CAST(column AS DECIMAL(38, 0))", message)
        self.assertIn("cast it to VARCHAR", message)

    def test_decimal_cast_shaped_value_can_be_consumed(self):
        """After SQL casts constrain precision and scale to an Arrow-compatible
        shape, values should be consumed as Decimal."""
        import jpype
        BigDecimal = jpype.JClass("java.math.BigDecimal")
        value = BigDecimal("123456789012345678901234567890123456.79")
        self.conn.jconn.mockHighPrecisionDecimalResult(value, 38, 2)

        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()

        self.assertEqual(result[0], Decimal("123456789012345678901234567890123456.79"))

    def test_decimal_integer_from_getObject(self):
        """Drivers like Oracle return BigDecimal with scale 0 for integer-like
        NUMERIC columns (e.g., NUMBER(10)). The vector now preserves the
        metadata's scale instead of inflating it, so the value round-trips
        without precision overflow."""
        self.conn.jconn.mockIntegerDecimalResult(42, 10, 0)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0], Decimal("42"))

    def test_numeric_type_mapping(self):
        """Types.NUMERIC should follow the same DECIMAL code path in
        ExplicitTypeMapper and DecimalConsumer."""
        import jpype
        self.conn.jconn.mockNumericTypeResult(
            jpype.JClass("java.math.BigDecimal")("99.99"), 10, 2)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0], Decimal("99.99"))

    # -- DECIMAL precision / scale combos --

    def test_decimal_scale_two(self):
        """DECIMAL(10, 2) — common financial precision."""
        import jpype
        BigDecimal = jpype.JClass("java.math.BigDecimal")
        self.conn.jconn.mockHighPrecisionDecimalResult(
            BigDecimal("12345.67"), 10, 2)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0], Decimal("12345.67"))

    def test_decimal_scale_four(self):
        """DECIMAL(15, 4) — higher fractional precision."""
        import jpype
        BigDecimal = jpype.JClass("java.math.BigDecimal")
        self.conn.jconn.mockHighPrecisionDecimalResult(
            BigDecimal("12345.6789"), 15, 4)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0], Decimal("12345.6789"))

    def test_decimal_scale_eight(self):
        """DECIMAL(18, 8) — many fractional digits."""
        import jpype
        BigDecimal = jpype.JClass("java.math.BigDecimal")
        self.conn.jconn.mockHighPrecisionDecimalResult(
            BigDecimal("0.00000001"), 18, 8)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0], Decimal("0.00000001"))

    def test_decimal_precision_equals_scale(self):
        """DECIMAL(4, 4) — precision equals scale, only fractional digits."""
        import jpype
        BigDecimal = jpype.JClass("java.math.BigDecimal")
        self.conn.jconn.mockHighPrecisionDecimalResult(
            BigDecimal("0.1234"), 4, 4)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0], Decimal("0.1234"))

    def test_decimal_small_precision(self):
        """DECIMAL(5, 2) — tight precision budget."""
        import jpype
        BigDecimal = jpype.JClass("java.math.BigDecimal")
        self.conn.jconn.mockHighPrecisionDecimalResult(
            BigDecimal("123.45"), 5, 2)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0], Decimal("123.45"))

    def test_decimal_large_precision_small_scale(self):
        """DECIMAL(30, 2) — wide integer part, minimal fraction."""
        import jpype
        BigDecimal = jpype.JClass("java.math.BigDecimal")
        self.conn.jconn.mockHighPrecisionDecimalResult(
            BigDecimal("12345678901234567890123456.12"), 30, 2)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0], Decimal("12345678901234567890123456.12"))

    def test_decimal_negative_value(self):
        """DECIMAL(10, 2) — negative value round-trip."""
        import jpype
        BigDecimal = jpype.JClass("java.math.BigDecimal")
        self.conn.jconn.mockHighPrecisionDecimalResult(
            BigDecimal("-99.99"), 10, 2)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0], Decimal("-99.99"))

    def test_decimal_scale_zero_high_precision(self):
        """DECIMAL(18, 0) — large integer stored as decimal."""
        import jpype
        BigDecimal = jpype.JClass("java.math.BigDecimal")
        self.conn.jconn.mockHighPrecisionDecimalResult(
            BigDecimal("123456789012345678"), 18, 0)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0], Decimal("123456789012345678"))

    def test_decimal_max_arrow_scale(self):
        """DECIMAL(38, 17) — max scale Arrow supports, value uses all digits."""
        import jpype
        BigDecimal = jpype.JClass("java.math.BigDecimal")
        self.conn.jconn.mockHighPrecisionDecimalResult(
            BigDecimal("12345678901234567.12345678901234567"), 38, 17)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0], Decimal("12345678901234567.12345678901234567"))

    def test_decimal_scale_one(self):
        """DECIMAL(10, 1) — single fractional digit."""
        import jpype
        BigDecimal = jpype.JClass("java.math.BigDecimal")
        self.conn.jconn.mockHighPrecisionDecimalResult(
            BigDecimal("1.0"), 10, 1)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0], Decimal("1.0"))

    def test_decimal_very_small_negative(self):
        """DECIMAL(10, 6) — very small negative value."""
        import jpype
        BigDecimal = jpype.JClass("java.math.BigDecimal")
        self.conn.jconn.mockHighPrecisionDecimalResult(
            BigDecimal("-0.000001"), 10, 6)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0], Decimal("-0.000001"))

    # -- NUMERIC precision / scale combos --

    def test_numeric_scale_zero(self):
        """NUMERIC(10, 0) — integer-like NUMERIC."""
        import jpype
        BigDecimal = jpype.JClass("java.math.BigDecimal")
        self.conn.jconn.mockNumericTypeResult(
            BigDecimal("12345"), 10, 0)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0], Decimal("12345"))

    def test_numeric_scale_two(self):
        """NUMERIC(10, 2) — standard monetary NUMERIC."""
        import jpype
        BigDecimal = jpype.JClass("java.math.BigDecimal")
        self.conn.jconn.mockNumericTypeResult(
            BigDecimal("99.99"), 10, 2)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0], Decimal("99.99"))

    def test_numeric_scale_four(self):
        """NUMERIC(15, 4) — higher fractional precision."""
        import jpype
        BigDecimal = jpype.JClass("java.math.BigDecimal")
        self.conn.jconn.mockNumericTypeResult(
            BigDecimal("12345.6789"), 15, 4)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0], Decimal("12345.6789"))

    def test_numeric_scale_eight(self):
        """NUMERIC(18, 8) — many fractional digits."""
        import jpype
        BigDecimal = jpype.JClass("java.math.BigDecimal")
        self.conn.jconn.mockNumericTypeResult(
            BigDecimal("0.00000001"), 18, 8)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0], Decimal("0.00000001"))

    def test_numeric_precision_equals_scale(self):
        """NUMERIC(4, 4) — only fractional digits."""
        import jpype
        BigDecimal = jpype.JClass("java.math.BigDecimal")
        self.conn.jconn.mockNumericTypeResult(
            BigDecimal("0.1234"), 4, 4)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0], Decimal("0.1234"))

    def test_numeric_negative_value(self):
        """NUMERIC(10, 2) — negative value."""
        import jpype
        BigDecimal = jpype.JClass("java.math.BigDecimal")
        self.conn.jconn.mockNumericTypeResult(
            BigDecimal("-99.99"), 10, 2)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0], Decimal("-99.99"))

    def test_numeric_null_value(self):
        """SQL NULL in a NUMERIC column should return None."""
        self.conn.jconn.mockNullNumericResult(10, 2)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertIsNone(result[0])

    def test_numeric_integer_from_getObject(self):
        """NUMERIC(10, 0) with integer-like BigDecimal (e.g. PostgreSQL NUMERIC)."""
        import jpype
        BigDecimal = jpype.JClass("java.math.BigDecimal")
        self.conn.jconn.mockNumericTypeResult(
            BigDecimal("42"), 10, 0)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0], Decimal("42"))

    def test_numeric_high_precision_overflow(self):
        """NUMERIC BigDecimal with scale > vector scale is safely rounded."""
        import jpype
        BigDecimal = jpype.JClass("java.math.BigDecimal")
        value = BigDecimal("123456789012345678.12345678901234567890")
        self.conn.jconn.mockNumericTypeResult(value, 38, 2)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0], Decimal("123456789012345678.12"))

    def test_numeric_true_precision_overflow_has_actionable_error(self):
        """NUMERIC values exceeding Arrow DECIMAL(38, 0) should fail with actionable error."""
        import jpype
        BigDecimal = jpype.JClass("java.math.BigDecimal")
        value = BigDecimal("123456789012345678901234567890123456789")
        self.conn.jconn.mockNumericTypeResult(value, 38, 0)

        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            with self.assertRaises(Exception) as cm:
                cursor.fetchone()

        message = str(cm.exception)
        self.assertIn("Could not convert DECIMAL/NUMERIC value", message)
        self.assertIn("Arrow DECIMAL(38, 0)", message)
        self.assertIn("Cast the column in SQL", message)

    def test_numeric_large_precision_small_scale(self):
        """NUMERIC(30, 2) — wide integer part, minimal fraction."""
        import jpype
        BigDecimal = jpype.JClass("java.math.BigDecimal")
        self.conn.jconn.mockNumericTypeResult(
            BigDecimal("12345678901234567890123456.12"), 30, 2)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0], Decimal("12345678901234567890123456.12"))

    def test_numeric_max_arrow_scale(self):
        """NUMERIC(38, 17) — max scale Arrow supports."""
        import jpype
        BigDecimal = jpype.JClass("java.math.BigDecimal")
        self.conn.jconn.mockNumericTypeResult(
            BigDecimal("12345678901234567.12345678901234567"), 38, 17)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0], Decimal("12345678901234567.12345678901234567"))

    def test_numeric_scale_one(self):
        """NUMERIC(10, 1) — single fractional digit."""
        import jpype
        BigDecimal = jpype.JClass("java.math.BigDecimal")
        self.conn.jconn.mockNumericTypeResult(
            BigDecimal("1.0"), 10, 1)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0], Decimal("1.0"))

    def test_numeric_very_small_negative(self):
        """NUMERIC(10, 6) — very small negative value."""
        import jpype
        BigDecimal = jpype.JClass("java.math.BigDecimal")
        self.conn.jconn.mockNumericTypeResult(
            BigDecimal("-0.000001"), 10, 6)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0], Decimal("-0.000001"))

    def test_sql_exception_on_execute(self):
        self.conn.jconn.mockExceptionOnExecute("java.sql.SQLException", "expected")
        with self.conn.cursor() as cursor:
            try:
                cursor.execute("dummy stmt")
                self.fail("expected exception")
            except jaydebeapiarrow.DatabaseError as e:
                self.assertEqual(str(e), "java.sql.SQLException: expected")

    def test_runtime_exception_on_execute(self):
        self.conn.jconn.mockExceptionOnExecute("java.lang.RuntimeException", "expected")
        with self.conn.cursor() as cursor:
            try:
                cursor.execute("dummy stmt")
                self.fail("expected exception")
            except jaydebeapiarrow.InterfaceError as e:
                # JPype 1.4.1: "java.lang.RuntimeException: expected"
                # JPype 1.7.0+: "java.lang.java.lang.RuntimeException: java.lang.RuntimeException: expected"
                self.assertIn("RuntimeException: expected", str(e))

    def test_sql_exception_on_commit(self):
        self.conn.jconn.mockExceptionOnCommit("java.sql.SQLException", "expected")
        try:
            self.conn.commit()
            self.fail("expected exception")
        except jaydebeapiarrow.DatabaseError as e:
            self.assertEqual(str(e), "java.sql.SQLException: expected")

    def test_runtime_exception_on_commit(self):
        self.conn.jconn.mockExceptionOnCommit("java.lang.RuntimeException", "expected")
        try:
            self.conn.commit()
            self.fail("expected exception")
        except jaydebeapiarrow.InterfaceError as e:
            # JPype 1.4.1: "java.lang.RuntimeException: expected"
            # JPype 1.7.0+: "java.lang.java.lang.RuntimeException: java.lang.RuntimeException: expected"
            self.assertIn("RuntimeException: expected", str(e))

    def test_sql_exception_on_rollback(self):
        self.conn.jconn.mockExceptionOnRollback("java.sql.SQLException", "expected")
        try:
            self.conn.rollback()
            self.fail("expected exception")
        except jaydebeapiarrow.DatabaseError as e:
            self.assertEqual(str(e), "java.sql.SQLException: expected")

    def test_runtime_exception_on_rollback(self):
        self.conn.jconn.mockExceptionOnRollback("java.lang.RuntimeException", "expected")
        try:
            self.conn.rollback()
            self.fail("expected exception")
        except jaydebeapiarrow.InterfaceError as e:
            # JPype 1.4.1: "java.lang.RuntimeException: expected"
            # JPype 1.7.0+: "java.lang.java.lang.RuntimeException: java.lang.RuntimeException: expected"
            self.assertIn("RuntimeException: expected", str(e))

    def test_cursor_with_statement(self):
        self.conn.jconn.mockType("INTEGER")
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            self.assertIsNotNone(cursor._connection)
        self.assertIsNone(cursor._connection)

    def test_connection_with_statement(self):
        with jaydebeapiarrow.connect('org.jaydebeapi.mockdriver.MockDriver',
                                       'jdbc:jaydebeapi://dummyurl') as conn:
            self.assertEqual(conn._closed, False)
        self.assertEqual(conn._closed, True)

    # --- _to_java() parameter binding tests ---

    def test_to_java_none(self):
        """None should pass through as Java null."""
        self.conn.jconn.mockSetObjectCapture()
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt", (None,))
        captured = self.conn.jconn.getCapturedSetObjectArgs()
        self.assertEqual(len(captured), 1)
        self.assertIsNone(captured[0][1])

    def test_to_java_bool(self):
        """bool should pass through unchanged."""
        self.conn.jconn.mockSetObjectCapture()
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt", (True,))
        captured = self.conn.jconn.getCapturedSetObjectArgs()
        self.assertEqual(len(captured), 1)
        self.assertEqual(captured[0][1], True)

    def test_to_java_bytes(self):
        """bytes should convert to Java byte[] array."""
        import jpype
        self.conn.jconn.mockSetObjectCapture()
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt", (b'\x00\x01\x02',))
        captured = self.conn.jconn.getCapturedSetObjectArgs()
        self.assertEqual(len(captured), 1)
        self.assertIsInstance(captured[0][1], jpype.JArray(jpype.JByte))

    def test_to_java_bytearray(self):
        """bytearray should convert to Java byte[] array."""
        import jpype
        self.conn.jconn.mockSetObjectCapture()
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt", (bytearray(b'\x03\x04\x05'),))
        captured = self.conn.jconn.getCapturedSetObjectArgs()
        self.assertEqual(len(captured), 1)
        self.assertIsInstance(captured[0][1], jpype.JArray(jpype.JByte))

    def test_to_java_datetime(self):
        """datetime should convert to java.sql.Timestamp."""
        import jpype
        Timestamp = jpype.JClass("java.sql.Timestamp")
        dt = datetime(2024, 6, 15, 10, 30, 45)
        self.conn.jconn.mockSetObjectCapture()
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt", (dt,))
        captured = self.conn.jconn.getCapturedSetObjectArgs()
        self.assertEqual(len(captured), 1)
        self.assertIsInstance(captured[0][1], Timestamp)

    def test_to_java_date(self):
        """date should convert to java.sql.Date."""
        import jpype
        Date = jpype.JClass("java.sql.Date")
        d = datetime(2024, 6, 15).date()
        self.conn.jconn.mockSetObjectCapture()
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt", (d,))
        captured = self.conn.jconn.getCapturedSetObjectArgs()
        self.assertEqual(len(captured), 1)
        self.assertIsInstance(captured[0][1], Date)

    def test_to_java_time(self):
        """time should convert to java.sql.Time."""
        import jpype
        Time = jpype.JClass("java.sql.Time")
        t = datetime(2024, 6, 15, 10, 30, 45).time()
        self.conn.jconn.mockSetObjectCapture()
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt", (t,))
        captured = self.conn.jconn.getCapturedSetObjectArgs()
        self.assertEqual(len(captured), 1)
        self.assertIsInstance(captured[0][1], Time)

    def test_to_java_decimal(self):
        """Decimal should convert to java.math.BigDecimal."""
        import jpype
        BigDecimal = jpype.JClass("java.math.BigDecimal")
        d = Decimal("123.456")
        self.conn.jconn.mockSetObjectCapture()
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt", (d,))
        captured = self.conn.jconn.getCapturedSetObjectArgs()
        self.assertEqual(len(captured), 1)
        self.assertIsInstance(captured[0][1], BigDecimal)
        self.assertEqual(str(captured[0][1]), "123.456")

    def test_to_java_int_passthrough(self):
        """int should pass through unchanged."""
        self.conn.jconn.mockSetObjectCapture()
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt", (42,))
        captured = self.conn.jconn.getCapturedSetObjectArgs()
        self.assertEqual(len(captured), 1)
        self.assertEqual(captured[0][1], 42)

    def test_to_java_float_passthrough(self):
        """float should pass through unchanged."""
        self.conn.jconn.mockSetObjectCapture()
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt", (3.14,))
        captured = self.conn.jconn.getCapturedSetObjectArgs()
        self.assertEqual(len(captured), 1)
        self.assertEqual(captured[0][1], 3.14)

    def test_to_java_str_passthrough(self):
        """str should pass through unchanged."""
        self.conn.jconn.mockSetObjectCapture()
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt", ("hello",))
        captured = self.conn.jconn.getCapturedSetObjectArgs()
        self.assertEqual(len(captured), 1)
        self.assertEqual(captured[0][1], "hello")

    def test_to_java_list_raises_not_supported(self):
        """list should raise NotSupportedError for ARRAY binding."""
        self.conn.jconn.mockSetObjectCapture()
        with self.conn.cursor() as cursor:
            with self.assertRaises(jaydebeapiarrow.NotSupportedError):
                cursor.execute("dummy stmt", ([1, 2, 3],))

    # --- DBAPITypeObject mapping tests ---

    def test_dbapi_type_other_maps_to_string(self):
        """JDBC OTHER should map to STRING type code."""
        import jpype
        Types = jpype.java.sql.Types
        result = jaydebeapiarrow.DBAPITypeObject._map_jdbc_type_to_dbapi(Types.OTHER)
        self.assertIs(result, jaydebeapiarrow.STRING)

    def test_dbapi_type_nclob_maps_to_text(self):
        """JDBC NCLOB should map to TEXT type code."""
        import jpype
        Types = jpype.java.sql.Types
        result = jaydebeapiarrow.DBAPITypeObject._map_jdbc_type_to_dbapi(Types.NCLOB)
        self.assertIs(result, jaydebeapiarrow.TEXT)

    def test_dbapi_type_sqlxml_maps_to_text(self):
        """JDBC SQLXML should map to TEXT type code."""
        import jpype
        Types = jpype.java.sql.Types
        result = jaydebeapiarrow.DBAPITypeObject._map_jdbc_type_to_dbapi(Types.SQLXML)
        self.assertIs(result, jaydebeapiarrow.TEXT)

    def test_dbapi_type_rowid_maps_to_rowid(self):
        """JDBC ROWID should map to ROWID type code."""
        import jpype
        Types = jpype.java.sql.Types
        result = jaydebeapiarrow.DBAPITypeObject._map_jdbc_type_to_dbapi(Types.ROWID)
        self.assertIs(result, jaydebeapiarrow.ROWID)

    # --- Timestamp millisecond leading-zero tests (legacy #175) ---

    def test_timestamp_ms_leading_zero_080(self):
        """Regression: .080 ms must not become .800 ms (legacy #175)."""
        import jpype
        LocalDateTime = jpype.JClass("java.time.LocalDateTime")
        ldt = LocalDateTime.of(2020, 1, 5, 11, 2, 14, 80_000_000)
        self.conn.jconn.mockTimestampResult(ldt)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0].microsecond, 80000)

    def test_timestamp_ms_leading_zero_009(self):
        """Timestamp with .009 ms — extreme leading-zero case."""
        import jpype
        LocalDateTime = jpype.JClass("java.time.LocalDateTime")
        ldt = LocalDateTime.of(2020, 6, 1, 0, 0, 0, 9_000_000)
        self.conn.jconn.mockTimestampResult(ldt)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0].microsecond, 9000)

    def test_timestamp_ms_leading_zero_007(self):
        """Timestamp with .007 ms — another leading-zero case."""
        import jpype
        LocalDateTime = jpype.JClass("java.time.LocalDateTime")
        ldt = LocalDateTime.of(2020, 3, 15, 12, 0, 0, 7_000_000)
        self.conn.jconn.mockTimestampResult(ldt)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0].microsecond, 7000)

    def test_timestamp_ms_no_leading_zero_743(self):
        """Timestamp with .743 ms — no leading zeros, should work correctly."""
        import jpype
        LocalDateTime = jpype.JClass("java.time.LocalDateTime")
        ldt = LocalDateTime.of(2020, 1, 7, 3, 25, 20, 743_000_000)
        self.conn.jconn.mockTimestampResult(ldt)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0].microsecond, 743000)
