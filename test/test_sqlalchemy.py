#-*- coding: utf-8 -*-

# Regression test for issue #115: DBAPITypeObject must be hashable so
# SQLAlchemy 2.x can use it as a cache key for result-set processors.
# Without __hash__, any query through SQLAlchemy raises
# `TypeError: unhashable type: 'DBAPITypeObject'` from deep inside
# SQLAlchemy's cursor result metadata construction.

import unittest

from decimal import Decimal

import jaydebeapiarrow

try:
    from test._base import _SUPPRESS_LOGGING_ARGS
except ImportError:
    from _base import _SUPPRESS_LOGGING_ARGS

try:
    import sqlalchemy
    from sqlalchemy import create_engine, text
    from sqlalchemy.dialects import registry as _dialect_registry
    from sqlalchemy.engine.default import DefaultDialect
    from sqlalchemy.pool import NullPool
    _HAS_SQLALCHEMY = True
except ImportError:
    _HAS_SQLALCHEMY = False


_DRIVER_CLASS = 'org.hsqldb.jdbcDriver'
_JDBC_URL = 'jdbc:hsqldb:mem:sa_issue115'
_DRIVER_ARGS = ['SA', '']


if _HAS_SQLALCHEMY:
    class JayDeBeApiArrowDialect(DefaultDialect):
        """Minimal SQLAlchemy dialect over jaydebeapiarrow.

        Exists only to drive the issue #115 regression test — not a full
        dialect implementation. Hands the JDBC driver class and URL from
        the engine URL's query string straight to jaydebeapiarrow.connect.
        """

        name = 'jaydebeapiarrow'
        paramstyle = 'qmark'
        default_paramstyle = 'qmark'
        supports_statement_cache = True

        @classmethod
        def dbapi(cls):
            return jaydebeapiarrow

        @classmethod
        def import_dbapi(cls):
            return jaydebeapiarrow

        def create_connect_args(self, url):
            driver_class = url.query.get('driver_class') or _DRIVER_CLASS
            jdbc_url = url.query.get('jdbc_url') or _JDBC_URL
            return (
                [driver_class, jdbc_url],
                {'driver_args': list(_DRIVER_ARGS)},
            )


@unittest.skipUnless(_HAS_SQLALCHEMY, "SQLAlchemy not installed")
class SQLAlchemyIntegrationTest(unittest.TestCase):
    """Issue #115: DBAPITypeObject must be hashable for SQLAlchemy 2.x."""

    @classmethod
    def setUpClass(cls):
        _dialect_registry.register(
            'jaydebeapiarrow', __name__, 'JayDeBeApiArrowDialect')

    def setUp(self):
        # Bootstrap the in-memory HSQLDB with a small table via raw
        # jaydebeapiarrow. The SQLAlchemy engine connects to the same
        # in-memory DB so it sees real cursor.description metadata.
        self._bootstrap = jaydebeapiarrow.connect(
            _DRIVER_CLASS, _JDBC_URL, _DRIVER_ARGS,
            jvm_args=_SUPPRESS_LOGGING_ARGS)
        with self._bootstrap.cursor() as cursor:
            cursor.execute("CREATE TABLE sa_test "
                           "(id INT, name VARCHAR(50), balance DECIMAL(10,2))")
            cursor.execute("INSERT INTO sa_test VALUES (1, 'alice', 12.40)")
            cursor.execute("INSERT INTO sa_test VALUES (2, 'bob', 12.90)")

        self.engine = create_engine(
            'jaydebeapiarrow:///?jdbc_url=%s&driver_class=%s'
            % (_JDBC_URL, _DRIVER_CLASS),
            poolclass=NullPool,
        )

    def tearDown(self):
        with self._bootstrap.cursor() as cursor:
            cursor.execute("DROP TABLE sa_test")
        self._bootstrap.close()
        self.engine.dispose()

    def test_select_does_not_raise_unhashable(self):
        """A SELECT through SQLAlchemy 2.x must succeed without
        `TypeError: unhashable type: 'DBAPITypeObject'`.

        Before the __hash__ fix, SQLAlchemy's result-processor cache
        (sqlalchemy.sql.type_api._cached_result_processor) raised while
        building per-column processors from cursor.description."""
        with self.engine.connect() as conn:
            result = conn.execute(
                text("SELECT id, name, balance FROM sa_test ORDER BY id"))
            rows = result.fetchall()
        self.assertEqual(rows, [(1, 'alice', Decimal('12.40')),
                                 (2, 'bob', Decimal('12.90'))])

    def test_repeated_select_hits_result_processor_cache(self):
        """A second SELECT reuses SQLAlchemy's cached result processors,
        which are keyed by the DBAPI type object — the exact path that
        broke before the fix."""
        with self.engine.connect() as conn:
            for _ in range(3):
                result = conn.execute(
                    text("SELECT id, name FROM sa_test ORDER BY id"))
                rows = result.fetchall()
                self.assertEqual(len(rows), 2)
