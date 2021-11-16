
import re

from adx_db.formatting import format


def handle_superset_custom_events(query: str):
    """
    Object: transform

    ```
    SELECT timestamp AS timestamp,
           name AS name
    FROM (customEvents |
          limit 10) AS expr_qry
    LIMIT 1000;
    ```
    to
    ```
    SELECT timestamp AS timestamp,
           name AS name
    FROM "customEvents | limit 10" AS expr_qry
    LIMIT 1000;
    ```
    because we always query customEvents in application_insight by superset,
    when do complex query, we have to write raw kql in SQL Lab in superset,
    so we want do handle mixture of sql and kql,
    but the above raw string cannot be parsed by moz_sql_parser, but if we do the above transformation, it works!
    :param query:
    :return:
    """
    start, end = 'FROM (customEvents', ') AS expr_qry'
    new_start, new_end = 'FROM "customEvents', '" AS expr_qry'
    start_index, end_index, len_end = query.find(start), query.find(end), len(end)

    if start_index > -1 and end_index > -1:
        query_kql = query[start_index: end_index + len_end].\
            replace('\n', ' ').\
            replace(start, new_start).\
            replace(end, new_end)

        query_new = query[:start_index] + query_kql + query[end_index + len_end:]

        query_new_strip = re.sub(r'\s+', ' ', query_new)
        return query_new_strip.strip()
    return query.strip()


def preprocess(query: str):

    query = handle_superset_custom_events(query)

    parsed_query_list = []
    for line in query.split('\n'):
        if line.startswith('--') or line.startswith('#') :
            pass
        else:
            parsed_query_list.append(line.strip())

    parsed_query = '\n'.join(parsed_query_list)

    print(parsed_query_list)

    return parsed_query.strip()


def translate(parsed_query: dict):
    translate_query = format(parsed_query)


    # todo below is for superset
    # (SELECT *
    #    from customEvents) AS expr_qry
    translate_query = translate_query.replace("expr_qry=(customEvents)", "customEvents")

    return translate_query


if __name__ == '__main__':
    sql = 'SELECT name FROM (customEvents | limit 10) AS expr_qry'

    print(repr(handle_superset_custom_events(sql)))
