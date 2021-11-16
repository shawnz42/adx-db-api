
import logging

from adx_db.exceptions import Error, NotSupportedError, ProgrammingError
from adx_db.query import execute

logger = logging.getLogger(__name__)


def connect(host: str = "",
            port: int = 80,
            path: str = "",
            scheme: str = "https",
            user: str = "",
            password: str = ""):
    """
    Constructor for creating a connection to the database.

        >>> conn = connect()
        >>> curs = conn.cursor()

    """
    return Connection(host, port, path, scheme, user, password)


class Connection(object):

    def __init__(self,
                 host: str = "",
                 port: int = 80,
                 path: str = "",
                 scheme: str = "https",
                 user: str = "",
                 password: str = ""):
        self.host = host
        self.port = port
        self.path = path
        self.scheme = scheme
        self.user = user
        self.password = password

        self.closed = False
        self.cursors = []

    def close(self):
        """Close the connection now."""
        self.closed = True
        for cursor in self.cursors:
            try:
                cursor.close()
            except Error:
                pass  # already closed

    def cursor(self):
        """Return a new Cursor Object using the connection."""
        cursor = Cursor(self.host, self.port, self.path, self.scheme,
                        self.user, self.password)
        self.cursors.append(cursor)

        return cursor

    def execute(self, operation, parameters=None, headers=0):
        cursor = self.cursor()
        return cursor.execute(operation, parameters, headers)

    def commit(self):
        """
        ADX doesn't support transactions.
        So just do nothing to support this method.
        """
        pass

    def __enter__(self):
        return self.cursor()

    def __exit__(self, *exc):
        self.close()


class Cursor(object):

    """Connection cursor."""

    def __init__(self,
                 host: str = "localhost",
                 port: int = 80,
                 path: str = "",
                 scheme: str = "https",
                 user: str = "",
                 password: str = "",
                 **kwargs
                 ):
        self.host = host
        self.port = port
        self.path = path
        self.scheme = scheme
        self.user = user
        self.password = password
        self.sql_path = kwargs.get("sql_path")

        # This read/write attribute specifies the number of rows to fetch at a
        # time with .fetchmany(). It defaults to 1 meaning to fetch a single
        # row at a time.
        self.arraysize = 1

        self.closed = False

        # this is updated only after a query
        self.description = None

        # this is set to a list of rows after a successful query
        self._results = None

    @property
    def rowcount(self):
        return len(self._results)

    def close(self):
        """Close the cursor."""
        self.closed = True

    def execute(self, operation, parameters=None, headers=0):
        print('operation: {} parameters: {} headers: {}'.format(operation, parameters, headers))

        self.description = None
        query = apply_parameters(operation, parameters or {})

        try:
            self._results, self.description = execute(
                query, headers, self.host, self.port, self.path, self.scheme, self.user, self.password)
        except (ProgrammingError, NotSupportedError) as e:
            print('e', e)

        return self

    def executemany(self, operation, seq_of_parameters=None):
        raise NotSupportedError('`executemany` is not supported, use `execute` instead')

    def fetchone(self):
        """
        Fetch the next row of a query result set, returning a single sequence,
        or `None` when no more data is available.
        """
        try:
            return self._results.pop(0)
        except IndexError:
            return None

    def fetchmany(self, size=None):
        """
        Fetch the next set of rows of a query result, returning a sequence of
        sequences (e.g. a list of tuples). An empty sequence is returned when
        no more rows are available.
        """
        size = size or self.arraysize
        out = self._results[:size]
        self._results = self._results[size:]
        return out

    def fetchall(self):
        """
        Fetch all (remaining) rows of a query result, returning them as a
        sequence of sequences (e.g. a list of tuples). Note that the cursor's
        arraysize attribute can affect the performance of this operation.
        """
        out = self._results[:]
        self._results = []
        return out

    def setinputsizes(self, sizes):
        # not supported
        pass

    def setoutputsizes(self, sizes):
        # not supported
        pass

    def __iter__(self):
        return iter(self._results)


def apply_parameters(operation, parameters):
    escaped_parameters = {
        key: escape(value) for key, value in parameters.items()
    }
    return operation % escaped_parameters


def escape(value):
    if value == '*':
        return value
    elif isinstance(value, str):
        return "'{}'".format(value.replace("'", "''"))
    elif isinstance(value, bool):
        return 'TRUE' if value else 'FALSE'
    elif isinstance(value, (int, float)):
        return str(value)
    elif isinstance(value, (list, tuple)):
        return '({0})'.format(', '.join(escape(element) for element in value))



