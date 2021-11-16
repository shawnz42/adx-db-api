# -*- coding: utf-8 -*-


import unittest

from sqlalchemy.engine.url import make_url

from .context import adx_db, AdxDialect


class DialectTestSuite(unittest.TestCase):

    def test_get_table_names(self):
        dialect = AdxDialect()

        url = make_url('adx://user:password@')
        connection = dialect.create_connect_args(url)

        result = dialect.get_table_names(connection)

        expected = [
           'customEvents'
        ]
        self.assertEqual(result, expected)



