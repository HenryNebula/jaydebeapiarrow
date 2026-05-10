#-*- coding: utf-8 -*-

import jaydebeapiarrow
import os
import unittest

try:
    from test._base import IntegrationTestBase, _THIS_DIR
except ImportError:
    from _base import IntegrationTestBase, _THIS_DIR


class MySQLTest(IntegrationTestBase, unittest.TestCase):

    def connect(self):

        import jpype

        host = os.environ.get("JY_MYSQL_HOST", "localhost")
        port = os.environ.get("JY_MYSQL_PORT", "13306")
        db_name = os.environ.get("JY_MYSQL_DB", "test_db")
        user = os.environ.get("JY_MYSQL_USER", "user")
        password = os.environ.get("JY_MYSQL_PASSWORD", "password")

        driver, url, driver_args = (
            'com.mysql.cj.jdbc.Driver',
            f'jdbc:mysql://{host}:{port}/{db_name}?user={user}&password={password}',
            None
        )

        try:
            db, conn = jaydebeapiarrow, self._quiet_connect(
                driver, url, driver_args)
        except jpype.JException as e:
            self.fail("Can not connect with MySQL. Please check if the instance is up and running.")
        else:
            return db, conn

    def setUpSql(self):
        self.sql_file(os.path.join(_THIS_DIR, 'data', 'create_mysql.sql'))
        self.sql_file(os.path.join(_THIS_DIR, 'data', 'insert.sql'))

    def _autoincrement_create_sql(self):
        return ("CREATE TABLE LASTROWID_TEST "
                "(id INT AUTO_INCREMENT PRIMARY KEY, "
                "val VARCHAR(50))")
