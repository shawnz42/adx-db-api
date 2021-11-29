
from adx_db.db import connect
from adx_db.exceptions import (
    DataError,
    DatabaseError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Warning,
)
from adx_db.parse import parse
from adx_db.translator import translate

# apilevel = '2.0'
# # Threads may share the module and connections
# threadsafety = 2
paramstyle = 'pyformat'


__all__ = [
    'connect',
    'apilevel',
    'threadsafety',
    'paramstyle',
    'DataError',
    'DatabaseError',
    'Error',
    'IntegrityError',
    'InterfaceError',
    'InternalError',
    'NotSupportedError',
    'OperationalError',
    'ProgrammingError',
    'Warning',
    'parse'
]


def get_kql(sql: str):
    return translate(parse(sql))


