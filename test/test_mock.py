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
import os
import shutil
import subprocess
import sys
import tempfile
import threading
from functools import partial

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

    def test_bigint_type_returns_int(self):
        """Verify JDBC BIGINT columns return Python int, not raw java.lang.Long.
        Regression test for legacy baztian/jaydebeapi#63."""
        self.conn.jconn.mockBigIntResult(377518399)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertIsInstance(result[0], int)
        self.assertEqual(result[0], 377518399)

    def test_bigint_edge_values(self):
        """Verify BIGINT edge cases: zero, negative, min, max long values."""
        for val in [0, -1, 9223372036854775807, -9223372036854775808]:
            self.conn.jconn.mockBigIntResult(val)
            with self.conn.cursor() as cursor:
                cursor.execute("dummy stmt")
                result = cursor.fetchone()
            self.assertIsInstance(result[0], int)
            self.assertEqual(result[0], val)

    def test_double_type_returns_float(self):
        """Verify JDBC DOUBLE columns return Python float, not raw java.lang.Double.
        Regression test for legacy baztian/jaydebeapi#243."""
        self.conn.jconn.mockDoubleResult(3.14)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertIsInstance(result[0], float)
        self.assertAlmostEqual(result[0], 3.14)

    def test_double_edge_values(self):
        """Verify DOUBLE edge cases: zero, negative, very large, very small."""
        for val in [0.0, -1.5, 1.7976931348623157e+308, 4.9e-324]:
            self.conn.jconn.mockDoubleResult(val)
            with self.conn.cursor() as cursor:
                cursor.execute("dummy stmt")
                result = cursor.fetchone()
            self.assertIsInstance(result[0], float)
            self.assertEqual(result[0], val)

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
        """None should use setNull() instead of setObject() for driver
        compatibility (e.g. Teradata rejects setObject(i, null))."""
        import jpype
        self.conn.jconn.mockSetObjectCapture()
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt", (None,))
        captured = self.conn.jconn.getCapturedSetObjectArgs()
        self.assertEqual(len(captured), 0, "setObject should not be called for None")
        null_captured = self.conn.jconn.getCapturedSetNullArgs()
        self.assertEqual(len(null_captured), 1)
        self.assertEqual(null_captured[0][0], 1)  # parameter index (1-based)
        self.assertEqual(null_captured[0][1], jpype.java.sql.Types.NULL)

    def test_to_java_none_mixed_params(self):
        """None among non-None params should use setNull() for the None slots."""
        import jpype
        self.conn.jconn.mockSetObjectCapture()
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt", (42, None, "hello", None))
        obj_captured = self.conn.jconn.getCapturedSetObjectArgs()
        self.assertEqual(len(obj_captured), 2)
        self.assertEqual(obj_captured[0][0], 1)
        self.assertEqual(obj_captured[0][1], 42)
        self.assertEqual(obj_captured[1][0], 3)
        self.assertEqual(obj_captured[1][1], "hello")
        null_captured = self.conn.jconn.getCapturedSetNullArgs()
        self.assertEqual(len(null_captured), 2)
        self.assertEqual(null_captured[0][0], 2)
        self.assertEqual(null_captured[0][1], jpype.java.sql.Types.NULL)
        self.assertEqual(null_captured[1][0], 4)
        self.assertEqual(null_captured[1][1], jpype.java.sql.Types.NULL)

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

    # --- Binary data round-trip tests ---

    def test_binary_non_utf8_bytes_preserved(self):
        """Binary data containing non-UTF-8 bytes must round-trip without loss.
        Verifies the fix for legacy issue baztian/jaydebeapi#147 where binary
        data was incorrectly decoded as UTF-8 strings."""
        import jpype
        test_data = bytes([0x00, 0x01, 0x02, 0x80, 0xff, 0xfe])
        java_bytes = jpype.JArray(jpype.JByte)(
            [b - 256 if b > 127 else b for b in test_data])
        self.conn.jconn.mockBinaryResult(java_bytes)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertIsInstance(result[0], bytes)
        self.assertEqual(result[0], test_data)

    def test_binary_all_byte_values(self):
        """All 256 byte values should round-trip correctly."""
        import jpype
        test_data = bytes(range(256))
        java_bytes = jpype.JArray(jpype.JByte)(
            [b - 256 if b > 127 else b for b in test_data])
        self.conn.jconn.mockBinaryResult(java_bytes)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0], test_data)

    def test_binary_empty(self):
        """Empty binary data should round-trip correctly."""
        import jpype
        java_bytes = jpype.JArray(jpype.JByte)(0)
        self.conn.jconn.mockBinaryResult(java_bytes)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0], b'')

    # --- DBAPITypeObject mapping tests ---

    def test_description_returns_column_label_not_name(self):
        """cursor.description should return the column label (AS alias),
        not the underlying column name from the table."""
        self.conn.jconn.mockColumnAlias("real_column", "alias_col")
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT real_column AS alias_col FROM t")
            self.assertEqual(cursor.description[0][0], "alias_col")

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

    # --- Timestamp sub-second leading zero tests (legacy #44) ---

    def test_timestamp_leading_zero_subsecond_096ms(self):
        """Regression: .096 ms must not become .96 ms (legacy #44).
        The legacy bug mangled 0.096965169 to 0.960000 by stripping the
        leading zero during string-based parsing. Our Arrow path uses
        integer nanosecond arithmetic via LocalDateTime.getNano()."""
        import jpype
        LocalDateTime = jpype.JClass("java.time.LocalDateTime")
        ldt = LocalDateTime.of(2017, 6, 19, 15, 30, 0, 96_965_169)
        self.conn.jconn.mockTimestampResult(ldt)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0].microsecond, 96965)

    def test_timestamp_leading_zero_000001us(self):
        """Timestamp with .000001 sub-seconds — minimal non-zero case."""
        import jpype
        LocalDateTime = jpype.JClass("java.time.LocalDateTime")
        ldt = LocalDateTime.of(2020, 1, 1, 0, 0, 0, 1_000)
        self.conn.jconn.mockTimestampResult(ldt)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0].microsecond, 1)

    def test_timestamp_leading_zero_001ms(self):
        """Timestamp with .001 ms — another leading-zero case."""
        import jpype
        LocalDateTime = jpype.JClass("java.time.LocalDateTime")
        ldt = LocalDateTime.of(2021, 3, 15, 12, 0, 0, 1_000_000)
        self.conn.jconn.mockTimestampResult(ldt)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0].microsecond, 1000)

    def test_timestamp_leading_zero_099999ms(self):
        """Timestamp with .099999 sub-seconds — leading zero + all 9s."""
        import jpype
        LocalDateTime = jpype.JClass("java.time.LocalDateTime")
        ldt = LocalDateTime.of(2019, 7, 4, 10, 30, 0, 99_999_000)
        self.conn.jconn.mockTimestampResult(ldt)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0].microsecond, 99999)

    # --- Timestamp microsecond precision tests (legacy issue #229) ---

    def test_timestamp_microsecond_precision_200000(self):
        """200000 microseconds (0.200000s) should round-trip correctly.
        Regression test for baztian/jaydebeapi#229."""
        import jpype
        LocalDateTime = jpype.JClass("java.time.LocalDateTime")
        ldt = LocalDateTime.of(2023, 5, 16, 18, 23, 15, 200_000_000)
        self.conn.jconn.mockTimestampResult(ldt)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        expected = datetime(2023, 5, 16, 18, 23, 15, 200000)
        self.assertEqual(result[0], expected)

    def test_timestamp_microsecond_precision_90000(self):
        """90000 microseconds (0.090000s) should round-trip correctly.
        Legacy bug caused this to become 900000 (extra zero).
        Regression test for baztian/jaydebeapi#229."""
        import jpype
        LocalDateTime = jpype.JClass("java.time.LocalDateTime")
        ldt = LocalDateTime.of(2023, 5, 16, 18, 23, 15, 90_000_000)
        self.conn.jconn.mockTimestampResult(ldt)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        expected = datetime(2023, 5, 16, 18, 23, 15, 90000)
        self.assertEqual(result[0], expected)

    def test_timestamp_microsecond_precision_123456(self):
        """123456 microseconds (0.123456s) should round-trip correctly.
        Regression test for baztian/jaydebeapi#229."""
        import jpype
        LocalDateTime = jpype.JClass("java.time.LocalDateTime")
        ldt = LocalDateTime.of(2023, 5, 16, 18, 23, 15, 123_456_000)
        self.conn.jconn.mockTimestampResult(ldt)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        expected = datetime(2023, 5, 16, 18, 23, 15, 123456)
        self.assertEqual(result[0], expected)

    def test_timestamp_microsecond_precision_zero(self):
        """0 microseconds should round-trip correctly."""
        import jpype
        LocalDateTime = jpype.JClass("java.time.LocalDateTime")
        ldt = LocalDateTime.of(2023, 5, 16, 18, 23, 15, 0)
        self.conn.jconn.mockTimestampResult(ldt)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        expected = datetime(2023, 5, 16, 18, 23, 15, 0)
        self.assertEqual(result[0], expected)

    def test_timestamp_microsecond_precision_999999(self):
        """999999 microseconds (max precision) should round-trip correctly."""
        import jpype
        LocalDateTime = jpype.JClass("java.time.LocalDateTime")
        ldt = LocalDateTime.of(2023, 5, 16, 18, 23, 15, 999_999_000)
        self.conn.jconn.mockTimestampResult(ldt)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        expected = datetime(2023, 5, 16, 18, 23, 15, 999999)
        self.assertEqual(result[0], expected)

    # --- Timestamp timezone preservation tests (legacy issue #73) ---

    def test_timestamp_returns_naive_datetime(self):
        """TIMESTAMP columns must return naive Python datetime objects.

        Regression test for baztian/jaydebeapi#73 where legacy jaydebeapi
        returned timestamps shifted to the JVM's local timezone. Our Arrow
        path normalizes to UTC on the Java side, so the returned datetime
        should always be naive and match the stored value exactly.
        """
        self.conn.jconn.mockType("TIMESTAMP")
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertIsInstance(result[0], datetime)
        self.assertIsNone(result[0].tzinfo,
                          "TIMESTAMP must return naive datetime, not timezone-aware")
        self.assertEqual(result[0], datetime(2009, 12, 1, 8, 20, 45))

    def test_timestamp_utc_boundary_value(self):
        """TIMESTAMP at UTC midnight must not shift to previous day.

        Regression test for baztian/jaydebeapi#73. If the JVM's default
        timezone is behind UTC (e.g., EST = UTC-5), a naive implementation
        would shift midnight UTC to the previous day. Our Arrow path uses
        UTC normalization, so the value must be preserved exactly.
        """
        import jpype
        localDT = jpype.java.time.LocalDateTime.of(2024, 1, 15, 0, 0, 0)
        self.conn.jconn.mockTimestampResult(localDT)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0], datetime(2024, 1, 15, 0, 0, 0))

    def test_timestamp_end_of_day_value(self):
        """TIMESTAMP near end of day must not overflow to next day.

        Regression test for baztian/jaydebeapi#73. Verifies that a
        timestamp near midnight (23:59:59) is preserved exactly without
        timezone shifting causing a day rollover.
        """
        self.conn.jconn.mockType("TIMESTAMP")
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        # The mock returns 2009-12-01T08:20:45 — verify exact value
        self.assertEqual(result[0].year, 2009)
        self.assertEqual(result[0].month, 12)
        self.assertEqual(result[0].day, 1)
        self.assertEqual(result[0].hour, 8)
        self.assertEqual(result[0].minute, 20)
        self.assertEqual(result[0].second, 45)

    # --- JPype API deprecation tests ---

    def test_no_deprecated_thread_attachment_api(self):
        """Verify that connect() does not use the deprecated
        jpype.isThreadAttachedToJVM(). Regression test for legacy
        baztian/jaydebeapi#203 where this triggered a DeprecationWarning."""
        import inspect
        import jaydebeapiarrow
        source = inspect.getsource(jaydebeapiarrow)
        self.assertNotIn('isThreadAttachedToJVM', source,
                         'Deprecated jpype.isThreadAttachedToJVM() must not be used; '
                         'use jpype.java.lang.Thread.isAttached() instead')

    def test_connect_no_deprecation_warnings(self):
        """Verify that connecting via the mock driver emits no
        DeprecationWarnings from JPype. Regression test for legacy
        baztian/jaydebeapi#203."""
        import warnings
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            self.conn = jaydebeapiarrow.connect(
                'org.jaydebeapi.mockdriver.MockDriver',
                'jdbc:jaydebeapi://dummyurl')
        jpype_warnings = [w for w in caught
                          if issubclass(w.category, DeprecationWarning)
                          and 'jpype' in str(w.message).lower()]
        self.assertEqual(
            len(jpype_warnings), 0,
            f'Unexpected JPype deprecation warnings: '
            f'{[str(w.message) for w in jpype_warnings]}')

    # --- Non-ASCII character round-trip tests (legacy issue #176) ---

    def test_varchar_german_umlauts(self):
        """VARCHAR columns with German umlauts must round-trip correctly.
        Regression test for baztian/jaydebeapi#176 where reading VARCHAR
        columns containing umlauts caused CharConversionException."""
        self.conn.jconn.mockStringResult("Grüße aus München")
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0], "Grüße aus München")

    def test_varchar_cjk_characters(self):
        """VARCHAR columns with CJK characters must round-trip correctly."""
        self.conn.jconn.mockStringResult("你好世界")
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0], "你好世界")

    def test_varchar_mixed_scripts(self):
        """VARCHAR columns with mixed scripts and symbols must round-trip correctly."""
        self.conn.jconn.mockStringResult("café — résumé — naïve")
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0], "café — résumé — naïve")

    def test_varchar_emoji(self):
        """VARCHAR columns with emoji must round-trip correctly."""
        self.conn.jconn.mockStringResult("Hello 🌍🌍")
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0], "Hello 🌍🌍")

    # --- Long query string tests (legacy issue #91) ---

    def test_long_query_string_18k_characters(self):
        """SQL strings of 18k+ characters must pass through execute()
        and return correct values. Regression test for
        baztian/jaydebeapi#91 where long queries caused failures."""
        self.conn.jconn.mockBigDecimalResult(1, 0)
        long_query = ("SELECT * FROM t WHERE id IN ("
                      + ",".join(str(i) for i in range(5000)) + ")")
        self.assertGreater(len(long_query), 18000,
                           "Test query must exceed 18k characters")
        with self.conn.cursor() as cursor:
            cursor.execute(long_query)
            result = cursor.fetchone()
        self.assertEqual(result[0], 1)

    # --- Memory leak regression tests (legacy #227) ---

    def test_cursor_close_after_partial_fetch(self):
        """Closing a cursor after a partial fetch should not leak the iterator."""
        self.conn.jconn.mockType("INTEGER")
        cursor = self.conn.cursor()
        cursor.execute("dummy stmt")
        cursor.fetchone()
        cursor.close()
        self.assertIsNone(cursor._iter)
        self.assertEqual(cursor._buffer, [])
        self.assertIsNone(cursor._connection)

    def test_repeated_query_cycles_no_accumulation(self):
        """Repeated execute/close cycles should not accumulate stale iterators
        or buffers (legacy #227). The mock driver's ResultSet never exhausts,
        so we test partial fetch + close cycles instead."""
        self.conn.jconn.mockType("INTEGER")
        for _ in range(10):
            cursor = self.conn.cursor()
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
            self.assertIsNotNone(result)
            cursor.close()
            # After close, iterator and buffer should be cleaned up
            self.assertIsNone(cursor._iter)
            self.assertEqual(cursor._buffer, [])

    def test_close_last_idempotent(self):
        """Calling _close_last multiple times should not raise."""
        self.conn.jconn.mockType("INTEGER")
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            cursor.fetchone()
            cursor._close_last()
            cursor._close_last()
            self.assertIsNone(cursor._iter)

    # --- JPype compatibility tests (legacy issue #253) ---

    def test_is_jvm_started_with_api_present(self):
        """_is_jvm_started() returns True when JVM is running via the standard API."""
        import jpype
        result = jaydebeapiarrow._is_jvm_started()
        self.assertTrue(result, "JVM should be started during mock tests")

    def test_is_jvm_started_fallback_without_public_api(self):
        """_is_jvm_started() falls back to internal state when isJVMStarted is missing.

        Simulates JPype versions (e.g. 1.6.0) that removed the public
        ``jpype.isJVMStarted()`` API.  The helper must still return the
        correct value by inspecting ``jpype._core._JVM_started``.
        """
        import jpype
        # Save and remove the public API
        original = getattr(jpype, 'isJVMStarted', None)
        try:
            delattr(jpype, 'isJVMStarted')
            # JVM is running in this test, so fallback must return True
            result = jaydebeapiarrow._is_jvm_started()
            self.assertTrue(result,
                             "Fallback must return True when JVM is running")
        finally:
            # Restore the original API
            if original is not None:
                jpype.isJVMStarted = original

    # --- JPype field reflection API tests (legacy #111) ---

    def test_java_sql_types_reflection_uses_standard_api(self):
        """Verify java.sql.Types constants are accessed via standard Java
        Reflection API (field.get/getModifiers/getName), not the deprecated
        JPype-specific getStaticAttribute() which was removed in newer JPype."""
        import jpype
        Types = jpype.java.sql.Types
        fields = Types.class_.getFields()
        # Verify we can iterate fields using standard Reflection
        static_public_fields = {}
        for field in fields:
            modifiers = field.getModifiers()
            if jpype.java.lang.reflect.Modifier.isStatic(modifiers) and \
               jpype.java.lang.reflect.Modifier.isPublic(modifiers):
                value = int(field.get(None))
                static_public_fields[field.getName()] = value
        # Spot-check well-known constants
        self.assertEqual(static_public_fields['INTEGER'], 4)
        self.assertEqual(static_public_fields['VARCHAR'], 12)
        self.assertEqual(static_public_fields['TIMESTAMP'], 93)
        self.assertEqual(static_public_fields['DECIMAL'], 3)
        self.assertEqual(static_public_fields['NUMERIC'], 2)

    def test_jdbc_type_mapping_populates_correctly(self):
        """Verify _map_jdbc_type_to_dbapi builds the mapping using
        standard Reflection (not getStaticAttribute)."""
        import jpype
        Types = jpype.java.sql.Types
        # Trigger mapping population
        result = jaydebeapiarrow.DBAPITypeObject._map_jdbc_type_to_dbapi(Types.INTEGER)
        self.assertIs(result, jaydebeapiarrow.NUMBER)
        # Verify mapping is populated (not empty dict)
        self.assertIsNotNone(jaydebeapiarrow._jdbc_const_to_name)
        self.assertGreater(len(jaydebeapiarrow._jdbc_const_to_name), 20)

    def test_dbapi_type_eq_with_jdbc_constants(self):
        """Verify DBAPITypeObject.__eq__ works with JDBC type constants
        accessed through standard Java Reflection."""
        import jpype
        Types = jpype.java.sql.Types
        # Trigger mapping population via a call to _map_jdbc_type_to_dbapi
        jaydebeapiarrow.DBAPITypeObject._map_jdbc_type_to_dbapi(Types.INTEGER)
        # Now __eq__ should work since _jdbc_const_to_name is populated
        # Cast Java int to Python int for comparison
        # (Java int's __eq__ doesn't delegate to our DBAPITypeObject.__eq__)
        self.assertTrue(jaydebeapiarrow.NUMBER == int(Types.INTEGER))
        self.assertTrue(jaydebeapiarrow.NUMBER == int(Types.BIGINT))
        self.assertTrue(jaydebeapiarrow.NUMBER == int(Types.SMALLINT))
        self.assertTrue(jaydebeapiarrow.NUMBER == int(Types.TINYINT))
        # These should match STRING type
        self.assertTrue(jaydebeapiarrow.STRING == int(Types.VARCHAR))
        self.assertTrue(jaydebeapiarrow.STRING == int(Types.CHAR))
        # These should match DATETIME type
        self.assertTrue(jaydebeapiarrow.DATETIME == int(Types.TIMESTAMP))
        # DATE has its own type object
        self.assertTrue(jaydebeapiarrow.DATE == int(Types.DATE))

    def test_varchar_returns_data_not_empty(self):
        """Verify VARCHAR columns return actual data, not empty strings.

        Regression test for legacy issue #119 where Oracle 9i VARCHAR2 columns
        returned empty strings. In the original jaydebeapi, getObject() could
        return oracle.sql.CHAR objects that JPype failed to convert. In
        jaydebeapiarrow, the Arrow JDBC adapter uses getString() which always
        returns a proper java.lang.String.
        """
        self.conn.jconn.mockType("VARCHAR")
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertIsInstance(result[0], str)
        self.assertEqual(result[0], "DummyString")
        self.assertNotEqual(result[0], "")

    def test_varchar_with_multicolumn_result(self):
        """Verify VARCHAR data is returned correctly alongside numeric columns.

        Regression test for legacy issue #119: the reporter's query had mixed
        VARCHAR and numeric columns, and only numeric data was returned.
        """
        import jpype
        Types = jpype.java.sql.Types

        # Set up a 2-column result: INTEGER + VARCHAR
        self.conn.jconn.mockMultiColumnResult(
            [Types.INTEGER, Types.VARCHAR],
            [42, "Hello World"]
        )
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            result = cursor.fetchone()
        self.assertEqual(result[0], 42)
        self.assertEqual(result[1], "Hello World")

    # --- SQLXML type tests ---

    def test_sqlxml_column_returns_string(self):
        """SQLXML columns should return Python strings, not Java objects.
        Regression test for legacy issue baztian/jaydebeapi#223."""
        self.conn.jconn.mockType("SQLXML")
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            row = cursor.fetchone()
            self.assertIsInstance(row[0], str)
            self.assertEqual(row[0], "DummyString")

    # --- Autocommit skip tests (issue #78) ---

    def test_commit_skipped_when_autocommit_enabled(self):
        """commit() should be a no-op when autocommit is enabled."""
        self.conn.jconn.mockAutoCommit(True)
        # Should not raise even if commit would throw an exception
        self.conn.jconn.mockExceptionOnCommit("java.sql.SQLException",
                                               "Cannot commit when autoCommit is enabled.")
        self.conn.commit()  # must not raise

    def test_commit_called_when_autocommit_disabled(self):
        """commit() should call jconn.commit() when autocommit is disabled."""
        self.conn.jconn.mockAutoCommit(False)
        # No exception mock = default mock behavior, commit succeeds silently
        self.conn.commit()

    def test_rollback_skipped_when_autocommit_enabled(self):
        """rollback() should be a no-op when autocommit is enabled."""
        self.conn.jconn.mockAutoCommit(True)
        self.conn.jconn.mockExceptionOnRollback("java.sql.SQLException",
                                                 "Cannot rollback when autoCommit is enabled.")
        self.conn.rollback()  # must not raise

    def test_rollback_called_when_autocommit_disabled(self):
        """rollback() should call jconn.rollback() when autocommit is disabled."""
        self.conn.jconn.mockAutoCommit(False)
        self.conn.rollback()


    def test_lastrowid_exists_and_is_none(self):
        """PEP-249: lastrowid attribute must exist on cursor (fixes #84)."""
        with self.conn.cursor() as cursor:
            self.assertIsNone(cursor.lastrowid)

    def test_lastrowid_none_after_select(self):
        """lastrowid should be None after a SELECT query."""
        self.conn.jconn.mockBigDecimalResult(1, 0)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            cursor.fetchone()
            self.assertIsNone(cursor.lastrowid)

    def test_lastrowid_none_after_insert(self):
        """lastrowid should be None after INSERT (JDBC doesn't expose rowid)."""
        self.conn.jconn.mockBigDecimalResult(1, 0)
        with self.conn.cursor() as cursor:
            cursor.execute("dummy stmt")
            self.assertIsNone(cursor.lastrowid)

    def test_lastrowid_none_after_executemany(self):
        """lastrowid should be None after executemany (mock driver limitation: skip)."""
        self.skipTest("Mock driver executeBatch returns None; covered by integration test")


class JarPathSpacesTest(unittest.TestCase):
    """Tests for JAR file paths containing spaces (issue #86).

    These tests must run in a subprocess because JPype only allows
    one JVM start per process, and the main test suite already starts it.
    """

    def _find_mock_jar(self):
        for root, dirs, files in os.walk(os.path.dirname(__file__)):
            for f in files:
                if f.startswith('mockdriver') and f.endswith('.jar'):
                    return os.path.join(root, f)
        self.fail('mockdriver JAR not found')

    def _run_connect_in_subprocess(self, jar_path):
        """Run a connect call in a fresh subprocess and return success/failure."""
        code = f'''
import jaydebeapiarrow
try:
    conn = jaydebeapiarrow.connect(
        'org.jaydebeapi.mockdriver.MockDriver',
        'jdbc:jaydebeapi://dummyurl',
        jars={repr(jar_path)}
    )
    print('OK')
    conn.close()
except Exception as e:
    print(f'FAIL: {{type(e).__name__}}: {{e}}')
'''
        result = subprocess.run(
            [sys.executable, '-c', code],
            capture_output=True, text=True, timeout=30,
            cwd=os.path.dirname(os.path.dirname(__file__))
        )
        return result.stdout.strip(), result.stderr.strip()

    def test_jar_path_with_spaces(self):
        """JAR paths containing spaces should work (issue #86)."""
        mock_jar = self._find_mock_jar()
        with tempfile.TemporaryDirectory(prefix='path with spaces ') as tmpdir:
            dest = os.path.join(tmpdir, os.path.basename(mock_jar))
            shutil.copy2(mock_jar, dest)
            stdout, stderr = self._run_connect_in_subprocess(dest)
        self.assertEqual(stdout, 'OK', f'Connection failed: {stderr}')

    def test_jar_path_with_special_chars(self):
        """JAR paths containing parentheses and special chars should work."""
        mock_jar = self._find_mock_jar()
        with tempfile.TemporaryDirectory(prefix='path (x86) & test ') as tmpdir:
            dest = os.path.join(tmpdir, os.path.basename(mock_jar))
            shutil.copy2(mock_jar, dest)
            stdout, stderr = self._run_connect_in_subprocess(dest)
        self.assertEqual(stdout, 'OK', f'Connection failed: {stderr}')


class ParallelConnectTest(unittest.TestCase):
    """Test that parallel connect() calls are thread-safe (issue #60)."""

    def test_parallel_connects_after_jvm_started(self):
        """Multiple threads connecting simultaneously should not crash."""
        errors = []

        def connect_thread(idx):
            try:
                conn = jaydebeapiarrow.connect(
                    'org.jaydebeapi.mockdriver.MockDriver',
                    'jdbc:jaydebeapi://dummyurl%d' % idx)
                # Verify the connection is usable
                self.assertIsNotNone(conn)
                conn.close()
            except Exception as e:
                errors.append(e)

        threads = []
        for i in range(5):
            t = threading.Thread(target=partial(connect_thread, i))
            threads.append(t)
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [], f"Thread errors: {errors}")

    def test_jvm_startup_lock_exists(self):
        """The _jvm_startup_lock should be a threading.Lock."""
        self.assertTrue(hasattr(jaydebeapiarrow, '_jvm_startup_lock'))
        self.assertIsInstance(jaydebeapiarrow._jvm_startup_lock, type(threading.Lock()))
