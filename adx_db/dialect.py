

from sqlalchemy.engine import default

import adx_db


class AdxDialect(default.DefaultDialect):

    # TODO: review these
    # http://docs.sqlalchemy.org/en/latest/core/internals.html#sqlalchemy.engine.interfaces.Dialect
    name = "adx"
    scheme = "https"
    driver = "rest"

    def __init__(self, *args, **kwargs):
        super(AdxDialect, self).__init__(*args, **kwargs)

    @classmethod
    def dbapi(cls):
        return adx_db

    def do_ping(self, dbapi_connection):
        return True

    def create_connect_args(self, url):

        # print(url)

        kwargs = {
            "host": url.host,
            "port": url.port or 443,
            "path": url.database,
            "scheme": self.scheme or "https",
            "user": url.username or None,
            "password": url.password or None,
        }

        print(kwargs)
        if url.query:
            kwargs.update(url.query)

        print('kwargs', kwargs)
        return ([], kwargs)
    #
    # def get_schema_names(self, connection, **kwargs):
    #     if self.url is None:
    #         return []
    #
    #     query = 'SELECT C, COUNT(C) FROM "{catalog}" GROUP BY C'.format(
    #         catalog=self.url
    #     )
    #     result = connection.execute(query)
    #     return [row[0] for row in result.fetchall()]
    #

    def has_table(self, connection, table_name, schema=None):
        return table_name in self.get_table_names(connection, schema)

    def get_table_names(self, connection, schema=None, **kwargs):
        return ['customEvents']

    def get_view_names(self, connection, schema=None, **kwargs):
        return []

    def get_table_options(self, connection, table_name, schema=None, **kwargs):
        return {}

    def get_columns(self, connection, table_name, schema=None, **kwargs):
        query = 'SELECT * FROM {table} LIMIT 1'.format(table=table_name)
        print('get_columns query', query)
        result = connection.execute(query)

        print('result', result)
        return [
            {'name': item[0], 'type': item[1]} for item in result._cursor_description()
        ]

    def get_pk_constraint(self, connection, table_name, schema=None, **kwargs):
        return {"constrained_columns": [], "name": None}

    def get_foreign_keys(self, connection, table_name, schema=None, **kwargs):
        return []

    def get_check_constraints(self, connection, table_name, schema=None, **kwargs):
        return []

    def get_table_comment(self, connection, table_name, schema=None, **kwargs):
        return {"text": ""}

    def get_indexes(self, connection, table_name, schema=None, **kwargs):
        return []

    def get_unique_constraints(self, connection, table_name, schema=None, **kwargs):
        return []

    def get_view_definition(self, connection, view_name, schema=None, **kwargs):
        pass

    def do_rollback(self, dbapi_connection):
        pass

    def _check_unicode_returns(sel, connection, additional_tests=None):
        return True

    def _check_unicode_description(self, connection):
        return True
