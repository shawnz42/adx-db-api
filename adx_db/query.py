
import logging

import pyparsing
from sqlalchemy import String
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder

from adx_db.parse import parse as parse_sql
from adx_db.convert import convert_rows
from adx_db.exceptions import InterfaceError, ProgrammingError
from adx_db.translator import translate, preprocess
from adx_db.utils import format_moz_error
from adx_db.column_type import column_type_dict


logger = logging.getLogger(__name__)


# def get_column_map(url, credentials=None):
#     query = 'SELECT * LIMIT 0'
#     result = run_query(url, query, credentials)
#     return OrderedDict(
#         sorted((col['label'], col['id']) for col in result['table']['cols']))


def run_query(host, port, path, scheme, user, password, query):
    headers = {
        "X-Api-Key": password,
        "Content-Type": "application/json; charset=utf-8"
    }

    host_url = "{}://{}".format(scheme, host)
    print("host_url: {} port: {} path: {} scheme: {} user: {} password: {}".format(host_url, port, path, scheme,
                                                                               user, password))
    print("query:", query)
    authority_id, db = path.split('/')

    print("authority_id: {} db: {}".format(authority_id, db))

    kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(host_url, user, password,
                                                                                authority_id)

    client = KustoClient(kcsb)

    response = client.execute(db, query)
    rows = response.primary_results[0].raw_rows
    columns = response.primary_results[0].raw_columns

    return rows, columns


def get_description_from_payload(payload):
    """
    Return description from a single row.
    """
    return [
        (
            col['ColumnName'],               # name
            column_type_dict.get(col['ColumnType'], String),  # type_code
            None,                       # [display_size]
            None,                       # [internal_size]
            None,                       # [precision]
            None,                       # [scale]
            True,                       # [null_ok]
        )
        for col in payload
    ]


def execute(query,
            headers: int = 0,
            host: str = "",
            port: int = 80,
            path: str = "/v1/apps",
            scheme: str = "https",
            user: str = "",
            password: str = ""):
    print('query0', query)
    query = preprocess(query)

    print('Original query: {}'.format(query))

    if query.lower().startswith('select'):
        try:
            parsed_query = parse_sql(query)
        except pyparsing.ParseException as e:
            raise ProgrammingError(format_moz_error(query, e))

        translated_query = translate(parsed_query)
        print('Translated query: {}'.format(translated_query))
    else:
        translated_query = query

    rows, columns = run_query(host, port, path, scheme, user, password, translated_query)

    cols = [each["ColumnName"] for each in columns]

    description = get_description_from_payload(columns)

    results = convert_rows(cols, rows)

    return results, description
