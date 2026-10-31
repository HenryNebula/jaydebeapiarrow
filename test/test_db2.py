
import os
import unittest
from decimal import Decimal

import jaydebeapiarrow

try:
    from test._base import _THIS_DIR, IntegrationTestBase
except ImportError:
    from _base import _THIS_DIR, IntegrationTestBase


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
            db, conn = jaydebeapiarrow, self._quiet_connect(
                driver, url, driver_args)
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
        product_name = 'Savings account'
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
