
from sqlalchemy import String, Numeric, INTEGER, DATETIME, FLOAT

column_type_dict = {
    'string': String,
    'dynamic': String,
    'int': INTEGER,
    'number': Numeric,
    'datetime': DATETIME
}

