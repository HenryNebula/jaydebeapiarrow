#-*- coding: utf-8 -*-
"""PEP 249 (DB-API 2.0) conformance checks for jaydebeapiarrow.

Structural and behavioral conformance of the module interface, exception
hierarchy, type objects/constructors, and the Connection/Cursor contracts,
driven against the MockDriver so the whole module runs in the driver-mock
CI environment.

Not covered here (by design):
- Row-exhaustion semantics (fetchone->None, fetchall/fetchmany->[] once
  exhausted): MockDriver's result set repeats its mocked row forever, so
  these can only be asserted against a real driver — see
  IntegrationTestBase in test/_base.py (test_execute_and_fetchone_after_end
  and friends), which runs on every real-driver test environment.
- SQL-level behavior (paramstyle substitution, errors on bad SQL): covered
  by test_mock.py and the driver suites.
"""

import time
import unittest
from unittest import mock

import jaydebeapiarrow


class ModuleInterfaceConformanceTest(unittest.TestCase):
    """PEP 249 'Module Interface' section."""

    def test_module_globals(self):
        self.assertIn(jaydebeapiarrow.apilevel, ('1.0', '2.0'))
        self.assertIn(jaydebeapiarrow.threadsafety, (0, 1, 2, 3))
        self.assertIn(jaydebeapiarrow.paramstyle,
                      ('qmark', 'numeric', 'named', 'format', 'pyformat'))

    def test_exception_hierarchy(self):
        db = jaydebeapiarrow
        for name in ('Error', 'Warning', 'InterfaceError', 'DatabaseError',
                     'DataError', 'OperationalError', 'IntegrityError',
                     'InternalError', 'ProgrammingError', 'NotSupportedError'):
            self.assertTrue(hasattr(db, name), name)
            self.assertTrue(issubclass(getattr(db, name), Exception), name)
        self.assertTrue(issubclass(db.Warning, Exception))
        self.assertTrue(issubclass(db.Error, Exception))
        self.assertTrue(issubclass(db.InterfaceError, db.Error))
        self.assertTrue(issubclass(db.DatabaseError, db.Error))
        for name in ('DataError', 'OperationalError', 'IntegrityError',
                     'InternalError', 'ProgrammingError', 'NotSupportedError'):
            self.assertTrue(issubclass(getattr(db, name), db.DatabaseError),
                            name)

    def test_type_constructors(self):
        db = jaydebeapiarrow
        d = db.Date(2026, 10, 27)
        self.assertEqual((d.year, d.month, d.day), (2026, 10, 27))
        t = db.Time(12, 30, 59)
        self.assertEqual((t.hour, t.minute, t.second), (12, 30, 59))
        self.assertIsNotNone(db.Timestamp(2026, 10, 27, 12, 30, 59))
        ticks = time.time()
        self.assertIsInstance(db.DateFromTicks(ticks), db.Date)
        self.assertIsInstance(db.TimeFromTicks(ticks), db.Time)
        self.assertIsInstance(db.TimestampFromTicks(ticks), db.Timestamp)
        self.assertIsInstance(db.Binary('héllo'), bytes)
        self.assertEqual(db.Binary(b'ab'), b'ab')

    def test_type_objects_exported_and_comparable(self):
        db = jaydebeapiarrow
        for name in ('STRING', 'BINARY', 'NUMBER', 'DATETIME', 'ROWID'):
            obj = getattr(db, name, None)
            self.assertIsNotNone(obj, name)
            # Comparable against JDBC type constants (int), the comparison
            # direction real consumers use.
            self.assertIsInstance(obj == db.NUMBER, bool)
        # The type objects compare equal to themselves by identity/group.
        self.assertEqual(db.STRING, db.STRING)


class ConnectionAndCursorConformanceTest(unittest.TestCase):
    """PEP 249 'Connection Objects' / 'Cursor Objects' contracts, on the
    MockDriver."""

    def setUp(self):
        self.conn = jaydebeapiarrow.connect(
            'org.jaydebeapi.mockdriver.MockDriver',
            'jdbc:jaydebeapi://dummyurl')

    def tearDown(self):
        self.conn.close()

    # --- Connection interface ------------------------------------------

    def test_connection_interface_methods(self):
        for method in ('close', 'commit', 'rollback', 'cursor'):
            self.assertTrue(callable(getattr(self.conn, method, None)), method)

    def test_commit_and_rollback_callable(self):
        self.conn.commit()
        self.conn.rollback()

    # --- Cursor: fresh-state contract -----------------------------------

    def test_cursor_required_methods(self):
        cursor = self.conn.cursor()
        try:
            for method in ('close', 'execute', 'executemany', 'fetchone',
                           'fetchmany', 'fetchall'):
                self.assertTrue(callable(getattr(cursor, method, None)),
                                method)
        finally:
            cursor.close()

    def test_description_is_none_before_first_execute(self):
        # PEP 249: "None ... if the cursor has not executed an operation
        # yet" — regression guard for issue #132.
        with self.conn.cursor() as cursor:
            self.assertIsNone(cursor.description)

    def test_fresh_cursor_rowcount_is_minus_one(self):
        # PEP 249: -1 in case no executeXX() has been performed.
        with self.conn.cursor() as cursor:
            self.assertEqual(cursor.rowcount, -1)

    def test_arraysize_defaults_to_one_and_is_writable(self):
        cursor = self.conn.cursor()
        try:
            self.assertEqual(cursor.arraysize, 1)
            cursor.arraysize = 5
            self.assertEqual(cursor.arraysize, 5)
        finally:
            cursor.close()

    def test_optional_methods_are_optional(self):
        # callproc/nextset MAY be absent; setinputsizes/setoutputsize MAY be
        # no-ops. Presence/absence is legal either way — just no crash.
        cursor = self.conn.cursor()
        try:
            cursor.setinputsizes(1)
            cursor.setoutputsize(1)
        finally:
            cursor.close()

    def test_fetch_before_execute_raises_error(self):
        # PEP 249: "An Error (or subclass) exception is raised if ...
        # no call to execute() was made."  Fetching from a cursor that has
        # never executed is unambiguous misuse and raises.  (A statement
        # that ran but produced no result set — DDL/DML — deliberately
        # stays lenient and returns None/[]; see the fetch_after_ddl tests
        # in test_hsqldb.)
        for method in ('fetchone', 'fetchmany', 'fetchall'):
            with self.conn.cursor() as cursor:
                with self.assertRaises(jaydebeapiarrow.Error, msg=method):
                    getattr(cursor, method)()

    # --- Cursor: after execute -------------------------------------------

    def test_execute_returns_none(self):
        # PEP 249 leaves execute()'s return value undefined; None is the
        # convention. Guard against accidentally returning rows.
        self.conn.jconn.mockStringResult('x')
        with self.conn.cursor() as cursor:
            self.assertIsNone(cursor.execute('dummy stmt'))

    def test_description_is_seven_tuple_with_string_name(self):
        self.conn.jconn.mockStringResult('x')
        with self.conn.cursor() as cursor:
            cursor.execute('dummy stmt')
            desc = cursor.description
        self.assertTrue(desc)
        for col in desc:
            self.assertEqual(len(col), 7)
        self.assertIsInstance(desc[0][0], str)

    def test_rowcount_minus_one_for_select_when_unknown(self):
        self.conn.jconn.mockStringResult('x')
        with self.conn.cursor() as cursor:
            cursor.execute('dummy stmt')
            self.assertEqual(cursor.rowcount, -1)

    def test_executemany_returns_none_and_survives_null_update_counts(self):
        # PEP 249 leaves executemany()'s return value undefined; rowcount is
        # -1 when not determinable.  The Mockito-backed MockDriver's
        # executeBatch() returns null — some real drivers do too — which
        # used to crash executemany() with a TypeError.
        # The Arrow batch binder cannot run against the Mockito mock's
        # unstubbed PreparedStatement, so parameter binding is bypassed:
        # this test targets executeBatch()'s null return, not binding.
        # mockStringResult activates the prepareStatement stubs (an unseeded
        # MockConnection returns null from prepareStatement).
        self.conn.jconn.mockStringResult('x')
        with mock.patch.object(
                jaydebeapiarrow.Cursor, '_set_stmt_parms',
                lambda self, stmt, params, is_batch=False: None):
            with self.conn.cursor() as cursor:
                ret = cursor.executemany('dummy stmt', [('a',), ('b',)])
        self.assertIsNone(ret)
        self.assertEqual(cursor.rowcount, -1)


if __name__ == '__main__':
    unittest.main()
