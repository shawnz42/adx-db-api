# -*- coding: utf-8 -*-

import unittest

from adx_db.parse import parse

from .context import translate


class TranslationTestSuite(unittest.TestCase):
    """
    some test cases are from https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/sqlcheatsheet
    """

    def test_select_column(self):
        sql = 'SELECT name FROM dependencies'
        expected = 'dependencies | project name'
        result = translate(parse(sql))
        self.assertEqual(result, expected)

    def test_select_columns(self):
        sql = 'SELECT name, resultCode FROM dependencies'
        expected = 'dependencies | project name, resultCode'
        result = translate(parse(sql))
        self.assertEqual(result, expected)

    def test_select_star(self):
        sql = 'SELECT * FROM dependencies'
        expected = 'dependencies'
        result = translate(parse(sql))
        self.assertEqual(result, expected)

    def test_function(self):
        sql = """select datetime('2021-02-17') from customEvents"""

        expected = "customEvents | project datetime('2021-02-17')"
        result = translate(parse(sql))
        self.assertEqual(result, expected)

    def test_functions(self):
        sql = """select datetime('2021-02-17'), ago('7d') from customEvents"""

        expected = "customEvents | project datetime('2021-02-17'), ago(7d)"
        result = translate(parse(sql))
        self.assertEqual(result, expected)

    def test_groupby_function(self):
        sql = """
        select bin(timestamp, '1d'), count(*) from customEvents
        group by bin(timestamp, '1d')
        """

        expected = "customEvents | summarize count() by bin(timestamp, 1d)"
        result = translate(parse(sql))
        self.assertEqual(result, expected)

    def test_groupby_alias(self):
        # todo auto find as in select
        sql = """
        select bin(timestamp, '1d') as day, count(*) from customEvents
        group by bin(timestamp, '1d') as day
        """

        expected = "customEvents | summarize count() by day=bin(timestamp, 1d)"
        result = translate(parse(sql))
        self.assertEqual(result, expected)

    def test_count_star(self):
        sql = """select count(*) from customEvents"""

        expected = "customEvents | summarize count()"
        result = translate(parse(sql))
        self.assertEqual(result, expected)

    def test_aggregate_columns(self):
        sql = """
        select COUNT(user_Id), SUM(itemCount)
        from customEvents
        """

        expected = "customEvents | summarize count(user_Id), sum(itemCount)"
        result = translate(parse(sql))
        self.assertEqual(result, expected)

    def test_union(self):
        sql = """
        SELECT name from customEvents limit 1
        union 
        SELECT name from customEvents limit 1
        union 
        SELECT name from customEvents limit 1
        """
        expected = '(customEvents | project name | limit 1) | union (customEvents | project name | limit 1) | union (customEvents | project name | limit 1)'
        result = translate(parse(sql))
        self.assertEqual(result, expected)

    def test_where(self):
        sql = 'SELECT * FROM dependencies WHERE type = "Azure blob"'
        expected = 'dependencies | where type == "Azure blob"'
        result = translate(parse(sql))
        self.assertEqual(result, expected)

    def test_substring(self):
        # NOT like '%blob%', but JUST 'bob'
        sql = "SELECT * FROM dependencies WHERE type like 'blob'"
        expected = "dependencies | where type contains 'blob'"
        result = translate(parse(sql))
        self.assertEqual(result, expected)

    def test_count(self):
        sql = 'SELECT count(user) FROM dependencies'
        expected = 'dependencies | summarize count(user)'
        result = translate(parse(sql))
        self.assertEqual(result, expected)

    def test_count_distinct(self):
        sql = 'SELECT count(distinct user) FROM dependencies'
        expected = 'dependencies | summarize dcount(user)'
        result = translate(parse(sql))
        self.assertEqual(result, expected)

    def test_limit_0(self):
        sql = 'SELECT * FROM customEvents LIMIT 0'
        expected = 'customEvents | limit 0'
        result = translate(parse(sql))
        self.assertEqual(result, expected)

    def test_limit_10(self):
        sql = 'SELECT * FROM customEvents LIMIT 10'
        expected = 'customEvents | limit 10'
        result = translate(parse(sql))
        self.assertEqual(result, expected)

    def test_nested_query(self):
        sql = """
        SELECT name, user_Id
        from (select name, user_Id from customEvents)
        """
        expected = '(customEvents | project name, user_Id) | project name, user_Id'
        result = translate(parse(sql))
        self.assertEqual(result, expected)

    def test_nested_query_with_alias(self):
        sql = """
        SELECT name AS name,
               timestamp AS timestamp
        FROM
          (SELECT name,
                  timestamp
           from customEvents
           limit 10) AS expr_qry
        LIMIT 1000;
        """
        expected = '(customEvents | project name, timestamp | limit 10) | project name=name, timestamp=timestamp | limit 1000'
        result = translate(parse(sql))
        self.assertEqual(result, expected)

    def test_groupby(self):
        sql = """
        SELECT bin(timestamp, '1d') AS __timestamp,
                name as new_name, 
               COUNT(*) AS count
        FROM "customEvents"
        WHERE timestamp >= datetime('2021-02-22 00:00:00')
          AND timestamp <= datetime('2021-03-01 00:00:00')
        GROUP BY bin(timestamp, '1d'), name
        ORDER BY count DESC
        LIMIT 10000;
        """

        expected = """customEvents | where timestamp >= datetime('2021-02-22 00:00:00') and timestamp <= datetime('2021-03-01 00:00:00') | summarize count=count() by new_name=name, bin(timestamp, 1d) | order by "count" desc | limit 10000"""

        result = translate(parse(sql))
        self.assertEqual(result, expected)

    def test_in(self):
        sql = """
        SELECT *
        from customEvents
        where name in ('error-handler', 'error.handler')"""

        expected = """customEvents | where name in ('error-handler', 'error.handler')"""

        result = translate(parse(sql))
        self.assertEqual(result, expected)

    def test_in_bad_case(self):
        # you should use ' but not " at in operation
        sql = """
        SELECT *
        from customEvents
        where name in ("error-handler", "error.handler")"""

        expected = """customEvents | where name in ("error-handler", error.handler)"""

        result = translate(parse(sql))
        self.assertEqual(result, expected)

    def test_json(self):
        sql = """
        SELECT tostring(customDimensions.version) AS version,
            bin(timestamp, '1d') AS __timestamp,
            count("user_Id") AS count_user_id
        FROM "customEvents"
        WHERE timestamp >= datetime('2021-02-22 00:00:00')
          AND timestamp <= datetime('2021-03-01 00:00:00')
        GROUP BY tostring(customDimensions.version),
                 bin(timestamp, '1d')
        ORDER BY count_user_id DESC
        LIMIT 10000;
        """
        expected = """customEvents | where timestamp >= datetime('2021-02-22 00:00:00') and timestamp <= datetime('2021-03-01 00:00:00') | summarize count_user_id=count(user_Id) by tostring(customDimensions.version), bin(timestamp, 1d) | order by count_user_id desc | limit 10000"""

        result = translate(parse(sql))
        self.assertEqual(result, expected)

    def test_function_like(self):
        sql = """
        SELECT bin(timestamp, '1d') AS __timestamp,
               COUNT(*) AS count
        FROM "customEvents"
        WHERE timestamp >= datetime('2020-12-02 00:00:00')
          AND timestamp <= datetime('2021-03-02 00:00:00')
          AND tostring(customDimensions.Locale) like 'US'
        GROUP BY bin(timestamp, '1d') AS __timestamp
        ORDER BY count DESC
        LIMIT 10000;
        """

        expected = """customEvents | where timestamp >= datetime('2020-12-02 00:00:00') and timestamp <= datetime('2021-03-02 00:00:00') and tostring(customDimensions.Locale) contains 'US' | summarize count=count() by __timestamp=bin(timestamp, 1d) | order by "count" desc | limit 10000"""

        result = translate(parse(sql))
        self.assertEqual(result, expected)

    def test_activity_metrics(self):
        sql = """
        select datetime('2021-03-01') as cal_date, datetime_diff('day', timestamp, datetime('2021-03-01')) as diff, retention_rate 
        from (select activity_metrics(user_Id, timestamp, datetime('2021-03-01'), datetime('2021-03-03'), '1d')
              from customEvents)
        """

        expected = """(customEvents | evaluate activity_metrics(user_Id, timestamp, datetime('2021-03-01'), datetime('2021-03-03'), 1d)) | project cal_date=datetime('2021-03-01'), diff=datetime_diff('day', timestamp, datetime('2021-03-01')), retention_rate"""

        result = translate(parse(sql))
        self.assertEqual(result, expected)

    def test_multi_agg(self):
        sql = """
        SELECT bin(timestamp, '1d') AS __timestamp,
               count("user_Id") AS event_count,
               count(DISTINCT "user_Id") AS user_count
        FROM "customEvents"
        WHERE timestamp >= datetime('2021-02-09 00:00:00')
          AND timestamp <= datetime('2021-03-09 00:00:00')
          AND name = 'moretools.openTi.click'
        GROUP BY bin(timestamp, '1d')
        ORDER BY event_count DESC
        LIMIT 10000;
        """

        expected = """customEvents | where timestamp >= datetime('2021-02-09 00:00:00') and timestamp <= datetime('2021-03-09 00:00:00') and name == 'moretools.openTi.click' | summarize event_count=count(user_Id), user_count=dcount(user_Id) by bin(timestamp, 1d) | order by event_count desc | limit 10000"""
        result = translate(parse(sql))
        self.assertEqual(result, expected)

    def test_dcountif_groupby(self):
        sql = """
        SELECT bin(timestamp, '1d') AS __timestamp,
               dcountif(user_Id, name=='moretools.openTi.click') AS user_count
        FROM "customEvents"
        GROUP BY bin(timestamp, '1d')
        """

        expected = """customEvents | summarize user_count=dcountif(user_Id, name == 'moretools.openTi.click') by bin(timestamp, 1d)"""
        result = translate(parse(sql))
        self.assertEqual(result, expected)

    def test_case_func(self):
        sql = """
            SELECT 
                case 
                    when substring(customDimensions.Locale, indexof(customDimensions.Locale, '_')+1) in ('US', 'CN') then substring(customDimensions.Locale, indexof(customDimensions.Locale, '_')+1)
                    else 'Others' 
                end
                as local_2
            FROM "customEvents"
         """

        expected = """customEvents | project local_2=case(substring(customDimensions.Locale, indexof(customDimensions.Locale, '_') + 1) in ('US', 'CN'),substring(customDimensions.Locale, indexof(customDimensions.Locale, '_') + 1),'Others')"""
        result = translate(parse(sql))
        self.assertEqual(result, expected)

    def test_dcountif(self):
        sql = """
        SELECT dcountif(user_Id, name=='moretools.openTi.click') AS click_open_ti_users,
               dcountif(user_Id, name=='page.moretools.open') AS open_moretools_users
        FROM "customEvents"
        """

        expected = """customEvents | summarize click_open_ti_users=dcountif(user_Id, name == 'moretools.openTi.click'), open_moretools_users=dcountif(user_Id, name == 'page.moretools.open')"""
        result = translate(parse(sql))
        self.assertEqual(result, expected)

    def test_kusto_reserved_keywords(self):
        sql = """
        SELECT COUNT(*) AS count
        FROM "customEvents"
        order by count
        """
        expected = 'customEvents | summarize count=count() | order by "count"'

        result = translate(parse(sql))
        self.assertEqual(result, expected)


if __name__ == '__main__':
    unittest.main()
