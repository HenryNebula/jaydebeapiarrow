#-*- coding: utf-8 -*-

import jaydebeapiarrow
import os
import unittest

from decimal import Decimal
from datetime import datetime
try:
    from test._base import IntegrationTestBase, _THIS_DIR
except ImportError:
    from _base import IntegrationTestBase, _THIS_DIR


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
            db, conn = jaydebeapiarrow, self._quiet_connect(
                driver, url, driver_args)
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

    def test_long_query_string_18k_characters(self):
        self.skipTest("Oracle has a 1000-element limit on IN clauses")

    def test_lastrowid_populated_for_identity_column(self):
        self.skipTest("Oracle JDBC returns ROWID instead of identity value via getGeneratedKeys")

    def test_varchar_columns_return_data(self):
        self.skipTest("Oracle requires TO_TIMESTAMP for date string literals")
