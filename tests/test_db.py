# -*- coding: utf-8 -*-

import unittest

from .context import (
    connect,
    exceptions,
    Connection
)


class DBTestSuite(unittest.TestCase):

    def test_connection(self):
        conn = connect()
        self.assertFalse(conn.closed)
        self.assertEqual(conn.cursors, [])

    def test_close_cursors(self):
        conn = connect()
        cursor1 = conn.cursor()
        cursor2 = conn.cursor()
        cursor2.close()

        conn.close()

        self.assertTrue(cursor1.closed)
        self.assertTrue(cursor2.closed)

    # def test_connection_execute(self, m):
    #     m.get(
    #         'http://docs.google.com/gviz/tq?gid=0&tq=SELECT%20%2A%20LIMIT%200',
    #         json=self.header_payload,
    #     )
    #     m.get(
    #         'http://docs.google.com/gviz/tq?gid=0&tq=SELECT%20%2A',
    #         json=self.query_payload,
    #     )
    #
    #     with Connection() as conn:
    #         result = conn.execute(
    #             'SELECT * FROM "http://docs.google.com/"').fetchall()
    #     Row = namedtuple('Row', 'country cnt')
    #     expected = [Row(country=u'BR', cnt=1.0), Row(country=u'IN', cnt=2.0)]
    #     self.assertEqual(result, expected)
    #
    # @requests_mock.Mocker()
    # def test_cursor_execute(self, m):
    #     m.get(
    #         'http://docs.google.com/gviz/tq?gid=0&tq=SELECT%20%2A%20LIMIT%200',
    #         json=self.header_payload,
    #     )
    #     m.get(
    #         'http://docs.google.com/gviz/tq?gid=0&tq=SELECT%20%2A',
    #         json=self.query_payload,
    #     )
    #
    #     with Connection() as conn:
    #         cursor = conn.cursor()
    #         result = cursor.execute(
    #             'SELECT * FROM "http://docs.google.com/"').fetchall()
    #     Row = namedtuple('Row', 'country cnt')
    #     expected = [Row(country=u'BR', cnt=1.0), Row(country=u'IN', cnt=2.0)]
    #     self.assertEqual(result, expected)
    #
    # def test_cursor_executemany(self):
    #     conn = Connection()
    #     cursor = conn.cursor()
    #     with self.assertRaises(exceptions.NotSupportedError):
    #         cursor.executemany('SELECT * FROM "http://docs.google.com/"')
    #
    # @requests_mock.Mocker()
    # def test_cursor(self, m):
    #     m.get(
    #         'http://docs.google.com/gviz/tq?gid=0&tq=SELECT%20%2A%20LIMIT%200',
    #         json=self.header_payload,
    #     )
    #     m.get(
    #         'http://docs.google.com/gviz/tq?gid=0&tq=SELECT%20%2A',
    #         json=self.query_payload,
    #     )
    #
    #     conn = Connection()
    #     cursor = conn.cursor()
    #     cursor.setinputsizes(0)   # no-op
    #     cursor.setoutputsizes(0)  # no-op
    #
    # @requests_mock.Mocker()
    # def test_cursor_rowcount(self, m):
    #     m.get(
    #         'http://docs.google.com/gviz/tq?gid=0&tq=SELECT%20%2A%20LIMIT%200',
    #         json=self.header_payload,
    #     )
    #     m.get(
    #         'http://docs.google.com/gviz/tq?gid=0&tq=SELECT%20%2A',
    #         json=self.query_payload,
    #     )
    #
    #     conn = Connection()
    #     cursor = conn.cursor()
    #
    #     with self.assertRaises(exceptions.Error):
    #         cursor.rowcount()
    #
    #     cursor.execute('SELECT * FROM "http://docs.google.com/"')
    #     self.assertEqual(cursor.rowcount, 2)
    #
    # @requests_mock.Mocker()
    # def test_cursor_fetchone(self, m):
    #     m.get(
    #         'http://docs.google.com/gviz/tq?gid=0&tq=SELECT%20%2A%20LIMIT%200',
    #         json=self.header_payload,
    #     )
    #     m.get(
    #         'http://docs.google.com/gviz/tq?gid=0&tq=SELECT%20%2A',
    #         json=self.query_payload,
    #     )
    #
    #     conn = Connection()
    #     cursor = conn.cursor()
    #     cursor.execute('SELECT * FROM "http://docs.google.com/"')
    #     Row = namedtuple('Row', 'country cnt')
    #
    #     self.assertEqual(cursor.fetchone(), Row(country=u'BR', cnt=1.0))
    #     self.assertEqual(cursor.fetchone(), Row(country=u'IN', cnt=2.0))
    #     self.assertIsNone(cursor.fetchone())
    #
    # @requests_mock.Mocker()
    # def test_cursor_fetchall(self, m):
    #     m.get(
    #         'http://docs.google.com/gviz/tq?gid=0&tq=SELECT%20%2A%20LIMIT%200',
    #         json=self.header_payload,
    #     )
    #     m.get(
    #         'http://docs.google.com/gviz/tq?gid=0&tq=SELECT%20%2A',
    #         json=self.query_payload,
    #     )
    #
    #     conn = Connection()
    #     cursor = conn.cursor()
    #     cursor.execute('SELECT * FROM "http://docs.google.com/"')
    #     Row = namedtuple('Row', 'country cnt')
    #
    #     self.assertEqual(cursor.fetchone(), Row(country=u'BR', cnt=1.0))
    #     self.assertEqual(cursor.fetchall(), [Row(country=u'IN', cnt=2.0)])
    #
    # @requests_mock.Mocker()
    # def test_cursor_fetchmany(self, m):
    #     m.get(
    #         'http://docs.google.com/gviz/tq?gid=0&tq=SELECT%20%2A%20LIMIT%200',
    #         json=self.header_payload,
    #     )
    #     m.get(
    #         'http://docs.google.com/gviz/tq?gid=0&tq=SELECT%20%2A',
    #         json=self.query_payload,
    #     )
    #
    #     conn = Connection()
    #     cursor = conn.cursor()
    #     cursor.execute('SELECT * FROM "http://docs.google.com/"')
    #     Row = namedtuple('Row', 'country cnt')
    #
    #     self.assertEqual(cursor.fetchmany(1), [Row(country=u'BR', cnt=1.0)])
    #     self.assertEqual(cursor.fetchmany(10), [Row(country=u'IN', cnt=2.0)])
    #     self.assertEqual(cursor.fetchmany(100), [])
    #
    # @requests_mock.Mocker()
    # def test_cursor_iter(self, m):
    #     m.get(
    #         'http://docs.google.com/gviz/tq?gid=0&tq=SELECT%20%2A%20LIMIT%200',
    #         json=self.header_payload,
    #     )
    #     m.get(
    #         'http://docs.google.com/gviz/tq?gid=0&tq=SELECT%20%2A',
    #         json=self.query_payload,
    #     )
    #
    #     conn = Connection()
    #     cursor = conn.cursor()
    #     cursor.execute('SELECT * FROM "http://docs.google.com/"')
    #     Row = namedtuple('Row', 'country cnt')
    #
    #     self.assertEqual(
    #         list(cursor),
    #         [Row(country=u'BR', cnt=1.0), Row(country=u'IN', cnt=2.0)],
    #     )
    #
    # def test_apply_parameters(self):
    #     query = 'SELECT * FROM table WHERE name=%(name)s'
    #     parameters = {'name': 'Alice'}
    #     result = apply_parameters(query, parameters)
    #     expected = "SELECT * FROM table WHERE name='Alice'"
    #     self.assertEqual(result, expected)
    #
    # def test_apply_parameters_escape(self):
    #     query = 'SELECT * FROM table WHERE name=%(name)s'
    #     parameters = {'name': "O'Malley's"}
    #     result = apply_parameters(query, parameters)
    #     expected = "SELECT * FROM table WHERE name='O''Malley''s'"
    #     self.assertEqual(result, expected)
    #
    # def test_apply_parameters_float(self):
    #     query = 'SELECT * FROM table WHERE age=%(age)s'
    #     parameters = {'age': 50}
    #     result = apply_parameters(query, parameters)
    #     expected = "SELECT * FROM table WHERE age=50"
    #     self.assertEqual(result, expected)
    #
    # def test_apply_parameters_bool(self):
    #     query = 'SELECT * FROM table WHERE active=%(active)s'
    #     parameters = {'active': True}
    #     result = apply_parameters(query, parameters)
    #     expected = "SELECT * FROM table WHERE active=TRUE"
    #     self.assertEqual(result, expected)
    #
    # def test_apply_parameters_list(self):
    #     query = (
    #         'SELECT * FROM table '
    #         'WHERE id IN %(allowed)s '
    #         'AND id NOT IN %(prohibited)s'
    #     )
    #     parameters = {'allowed': [1, 2], 'prohibited': (2, 3)}
    #     result = apply_parameters(query, parameters)
    #     expected = (
    #         'SELECT * FROM table '
    #         'WHERE id IN (1, 2) '
    #         'AND id NOT IN (2, 3)'
    #     )
    #     self.assertEqual(result, expected)
    #
    # def test_apply_parameters_star(self):
    #     query = 'SELECT %(column)s FROM table'
    #     parameters = {'column': '*'}
    #     result = apply_parameters(query, parameters)
    #     expected = "SELECT * FROM table"
    #     self.assertEqual(result, expected)
