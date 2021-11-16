# -*- coding: utf-8 -*-

import unittest

from .context import preprocess, handle_superset_custom_events


class PreprocessTestSuite(unittest.TestCase):

    def test_handle_superset_custom_events_1(self):
        sql = 'SELECT name FROM dependencies'
        expected = 'SELECT name FROM dependencies'
        result = handle_superset_custom_events(sql)
        self.assertEqual(result, expected)

    def test_handle_superset_custom_events_2(self):
        sql = 'SELECT name FROM (customEvents | limit 10) AS expr_qry'
        expected = 'SELECT name FROM "customEvents | limit 10" AS expr_qry'
        result = handle_superset_custom_events(sql)
        self.assertEqual(result, expected)

    def test_preprocess_1(self):
        sql = 'SELECT name FROM dependencies'
        expected = 'SELECT name FROM dependencies'
        result = preprocess(sql)
        self.assertEqual(result, expected)

    def test_preprocess_2(self):
        sql = 'SELECT name FROM (customEvents | limit 10) AS expr_qry'
        expected = 'SELECT name FROM "customEvents | limit 10" AS expr_qry'
        result = preprocess(sql)
        self.assertEqual(result, expected)


if __name__ == '__main__':
    unittest.main()
