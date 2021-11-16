# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Beto Dealmeida (beto@dealmeida.net)
#

import re

from mo_future import string_types, text, first, long, is_text

from adx_db.keywords import (RESERVED, kusto_reserved_keywords, join_keywords,
                             precedence, binary_ops, AGGREGATE_FUNCTIONS,
                             OPERATOR_PLUGINS)

VALID = re.compile(r'^[a-zA-Z_]\w*$')


def format(json, **kwargs):
    return Formatter(**kwargs).format(json)


def should_quote(identifier):
    """
    Return true if a given identifier should be quoted.

    This is usually true when the identifier:

      - is a reserved word
      - contain spaces
      - does not match the regex `[a-zA-Z_]\\w*`

    """
    return (
        identifier != '*' and (
            not VALID.match(identifier) or identifier in kusto_reserved_keywords))


def split_field(field):
    """
    RETURN field AS ARRAY OF DOT-SEPARATED FIELDS
    """
    if field == "." or field==None:
        return []
    elif is_text(field) and "." in field:
        if field.startswith(".."):
            remainder = field.lstrip(".")
            back = len(field) - len(remainder) - 1
            return [-1]*back + [k.replace("\a", ".") for k in remainder.replace("\\.", "\a").split(".")]
        else:
            return [k.replace("\a", ".") for k in field.replace("\\.", "\a").split(".")]
    else:
        return [field]


def join_field(path):
    """
    RETURN field SEQUENCE AS STRING
    """
    output = ".".join([f.replace(".", "\\.") for f in path if f != None])
    return output if output else "."


def escape(ident, ansi_quotes, should_quote):
    """
    Escape identifiers.

    ANSI uses single quotes, but many databases use back quotes.

    """
    def esc(identifier):
        if not should_quote(identifier):
            return identifier

        quote = '"' if ansi_quotes else '`'
        identifier = identifier.replace(quote, 2*quote)
        return '{0}{1}{2}'.format(quote, identifier, quote)
    return join_field(esc(f) for f in split_field(ident))


def Operator(op):
    prec = precedence[binary_ops[op]]
    op = ' {0} '.format(op).lower()

    def func(self, json):
        acc = []

        for v in json:
            sql = self.dispatch(v)
            if isinstance(v, (text, int, float, long)):
                acc.append(sql)
                continue

            p = precedence.get(first(v.keys()))
            if p is None:
                acc.append(sql)
                continue
            if p>=prec:
                acc.append("(" + sql + ")")
            else:
                acc.append(sql)
        return op.join(acc)
    return func


class Formatter:

    clauses = [
        'with_',
        'from_',
        'where',
        'having',
        'offset',
        'project',
        'groupby',
        'orderby',
        'limit',
    ]

    # simple operators
    _concat = Operator('||')
    _mul = Operator('*')
    _div = Operator('/')
    _mod = Operator('%')
    _add = Operator('+')
    _sub = Operator('-')
    _neq = Operator('<>')
    _gt = Operator('>')
    _lt = Operator('<')
    _gte = Operator('>=')
    _lte = Operator('<=')
    _eq = Operator('==')
    _or = Operator('or')
    _and = Operator('and')
    _binary_and = Operator("&")
    _binary_or = Operator("|")

    def __init__(self, ansi_quotes=True, should_quote=should_quote):
        self.ansi_quotes = ansi_quotes
        self.should_quote = should_quote

    def format(self, json):
        if 'union' in json:
            return self.union(json['union'])
        elif 'union_all' in json:
            return self.union_all(json['union_all'])
        else:
            return self.query(json)

    def dispatch(self, json):
        if isinstance(json, list):
            return self.delimited_list(json)
        if isinstance(json, dict):
            if len(json) == 0:
                return ''
            elif 'value' in json:
                return self.value(json)
            elif 'from' in json:
                # Nested queries
                return '({})'.format(self.format(json))
            elif 'select' in json:
                # Nested queries
                return '({})'.format(self.format(json))
            else:
                return self.op(json)
        if isinstance(json, string_types):
            return escape(json, self.ansi_quotes, self.should_quote)

        return text(json)

    def delimited_list(self, json):
        # return ', '.join(self.dispatch(element) for element in json)
        res_list = []
        for element in json:
            element_dispatched = self.dispatch(element)
            res_list.append(element_dispatched)

        return ', '.join(res_list)

    def value(self, json):
        parts = []
        if 'name' in json:
            parts.extend([self.dispatch(json['name']), '='])
        parts.extend([self.dispatch(json['value'])])
        return ''.join(parts)

    def op(self, json):
        if 'on' in json:
            return self._on(json)

        if len(json) > 1:
            raise Exception('Operators should have only one key!')
        key, value = list(json.items())[0]

        # check if the attribute exists, and call the corresponding method;
        # note that we disallow keys that start with `_` to avoid giving access
        # to magic methods
        attr = '_{0}'.format(key)
        if hasattr(self, attr) and not key.startswith('_'):
            method = getattr(self, attr)
            return method(value)

        # treat as regular function call
        if isinstance(value, dict) and len(value) == 0:
            return key.lower() + "()"  # NOT SURE IF AN EMPTY dict SHOULD BE DELT WITH HERE, OR IN self.dispatch()
        else:
            value_ = self.dispatch(value)
            if value_ == '*':
                value_ = ''
            return '{0}({1})'.format(key.lower(), value_)

    def _activity_metrics(self, value):
        # https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/activity-metrics-plugin
        # value: ['user_Id', 'timestamp', {'datetime': {'literal': '2021-02-17'}}, {'datetime': {'literal': '2021-02-25'}}, {'literal': '1d'}]
        return 'activity_metrics({0}, {1})'.format(self.dispatch(value[:4]), value[-1]['literal'])

    def _count(self, value):

        if isinstance(value, string_types):
            return 'count({0})'.format('' if value == '*' else value)
        elif isinstance(value, dict) and list(value.keys())[0].lower() == 'distinct':
            return 'dcount({0})'.format(list(value.values())[0])
        else:
            return 'count({0})'.format(value)

    def _ago(self, value):
        # value: {'literal': '7d'}
        return 'ago({0})'.format(value['literal'])

    def _bin(self, value):
        # value: ['timestamp', {'literal': '1d'}]
        return 'bin({0}, {1})'.format(value[0], value[1]['literal'])

    def _binary_not(self, value):
        return '~{0}'.format(self.dispatch(value))

    def _exists(self, value):
        return '{0} IS NOT NULL'.format(self.dispatch(value))

    def _missing(self, value):
        return '{0} IS NULL'.format(self.dispatch(value))

    def _like(self, pair):
        column, value = pair
        #
        # if isinstance(value, string_types):
        #     value = re.sub(r'^%|%$', '', value)
        # elif isinstance(value, dict):
        #     value = re.sub(r'^%|%$', '', value['literal'])

        return '{0} contains {1}'.format(self.dispatch(column), self.dispatch(value))

    def _nlike(self, pair):
        column, value = pair
        print(column, value)

        # if isinstance(value, string_types):
        #     value = re.sub(r'^%|%$', '', value)
        # elif isinstance(value, dict):
        #     value = re.sub(r'^%|%$', '', value['literal'])

        return '{0} not contains {1}'.format(self.dispatch(column), self.dispatch(value))

    def _is(self, pair):
        return '{0} is {1}'.format(self.dispatch(pair[0]), self.dispatch(pair[1]))

    def _in(self, json):
        print('in', json)
        valid = self.dispatch(json[1])
        print('valid', valid)
        # `(10, 11, 12)` does not get parsed as literal, so it's formatted as
        # `10, 11, 12`. This fixes it.
        if not valid.startswith('('):
            valid = '({0})'.format(valid)

        return '{0} in {1}'.format(self.dispatch(json[0]), valid)

    def _nin(self, json):
        valid = self.dispatch(json[1])
        # `(10, 11, 12)` does not get parsed as literal, so it's formatted as
        # `10, 11, 12`. This fixes it.
        if not valid.startswith('('):
            valid = '({0})'.format(valid)

        return '{0} not in {1}'.format(json[0], valid)

    def _case(self, checks):
        parts = []
        print('checks', checks)
        for check in checks:
            print('check', check)
            if isinstance(check, dict):
                if 'when' in check and 'then' in check:
                    parts.extend([self.dispatch(check['when'])])
                    parts.extend([self.dispatch(check['then'])])
                else:
                    parts.extend([self.dispatch(check)])
            else:
                parts.extend([self.dispatch(check)])
        return 'case(' + ','.join(parts) + ')'

    def _literal(self, json):
        if isinstance(json, list):
            return '({0})'.format(', '.join(self._literal(v) for v in json))
        elif isinstance(json, string_types):
            return "'{0}'".format(json.replace("'", "''"))
        else:
            return str(json)

    def _between(self, json):
        return '{0} between {1} and {2}'.format(self.dispatch(json[0]), self.dispatch(json[1]), self.dispatch(json[2]))

    def _not_between(self, json):
        return '{0} NOT between {1} and {2}'.format(self.dispatch(json[0]), self.dispatch(json[1]), self.dispatch(json[2]))

    def _on(self, json):
        detected_join = join_keywords & set(json.keys())
        if len(detected_join) == 0:
            raise Exception(
                'Fail to detect join type! Detected: "{}" Except one of: "{}"'.format(
                    [on_keyword for on_keyword in json if on_keyword != 'on'][0],
                    '", "'.join(join_keywords)
                )
            )

        join_keyword = detected_join.pop()

        return '{0} {1} ON {2}'.format(
            join_keyword.upper(), self.dispatch(json[join_keyword]), self.dispatch(json['on'])
        )

    def union(self, json):
        return '(' + ') | union ('.join(self.query(query) for query in json) + ')'

    def union_all(self, json):
        return '(' + ') | union ('.join(self.query(query) for query in json) + ')'

    def query(self, json):

        res_list = []
        for clause in self.clauses:
            for part in [getattr(self, clause)(json)]:
                if part:
                    res_list.append(part)

        res = ' '.join(res_list)

        return res

    def with_(self, json):
        if 'with' in json:
            with_ = json['with']
            if not isinstance(with_, list):
                with_ = [with_]
            parts = ', '.join(
                '{0} AS {1}'.format(part['name'], self.dispatch(part['value']))
                for part in with_
            )
            return 'WITH {0}'.format(parts)

    def select(self, json):
        if 'select' in json:
            return 'SELECT {0}'.format(self.dispatch(json['select']))

    def from_(self, json):
        is_join = False
        if 'from' in json:
            from_ = json['from']
            print('from_', from_)

            # # for superset SQL Lab generate mixture of sql and kql
            if (isinstance(from_, dict)
                    and from_.get('name', '') == 'expr_qry'
                    and isinstance(from_.get('value', ''), string_types)
                    and from_.get('value', '').find('|') > 0
                    and from_.get('value', '').startswith('customEvents')):
                return from_['value']

            if isinstance(from_, dict):
                from_.pop('name', None)  # just ignore the alias of nested query to avoid let state in kql(kusto)

            if 'union' in from_:
                return self.union(from_['union'])
            if not isinstance(from_, list):
                from_ = [from_]

            parts = []
            for token in from_:
                if join_keywords & set(token):
                    is_join = True
                parts.append(self.dispatch(token))
            joiner = ' ' if is_join else ', '
            rest = joiner.join(parts)

            print('from:', rest)

            return rest

    def where(self, json):
        if 'where' in json:
            return '| where {0}'.format(self.dispatch(json['where']))

    def groupby(self, json):
        """
        :param json:
        :return:
        """
        if 'groupby' in json:
            groupby_ = json['groupby']
            if isinstance(groupby_, dict):
                groupby_ = [groupby_]

            select_ = json['select']
            if isinstance(select_, dict):
                select_ = [select_]

            groupby_value_list = [item['value'] for item in groupby_]

            # get aggregation columns from select
            print("json['select']", json['select'])
            aggregation = [item for item in select_ if (isinstance(item['value'], dict)
                                                               and item['value'] not in groupby_value_list)]

            # get group by columns from select(alias) and groupby
            select = [item for item in select_ if isinstance(item['value'], string_types)]
            select_value_list = [item['value'] for item in select]

            groupby = select + [item for item in groupby_ if item['value'] not in select_value_list]

            res_list = []
            for each in aggregation:
                # agg = '{}'.format(self.dispatch(each))
                res_list.append(each)

            print('self.dispatch(res_list)', self.dispatch(res_list))
            if res_list:
                return '| summarize {} by {}'.format(self.dispatch(res_list).replace('"', ''), self.dispatch(groupby))
            else:
                return '| summarize by {}'.format(self.dispatch(select)).replace('"', '')

    def having(self, json):
        if 'having' in json:
            return 'HAVING {0}'.format(self.dispatch(json['having']))

    def orderby(self, json):
        if 'orderby' in json:
            orderby = json['orderby']
            if isinstance(orderby, dict):
                orderby = [orderby]
            return '| order by {0}'.format(','.join([
                '{0} {1}'.format(self.dispatch(o), o.get('sort', '').lower()).strip()
                for o in orderby
            ]))

    def limit(self, json):
        if 'limit' in json:
            if json['limit'] >= 0:
                return '| limit {0}'.format(self.dispatch(json['limit']))

    def offset(self, json):
        if 'offset' in json:
            return 'OFFSET {0}'.format(self.dispatch(json['offset']))

    def project(self, json):
        if 'groupby' in json:
            return

        if 'select' in json:
            select = json['select']
            # print('select', select)  # debug
            if select == '*':
                return ''

            if isinstance(select, dict):
                select = [select]

            scalar_columns = []
            aggregate_columns = []
            operate_plugins = []

            for item in select:
                if isinstance(item['value'], string_types):
                    scalar_columns.append(item)
                elif isinstance(item['value'], dict):
                    if list(item['value'].keys())[0].lower() in AGGREGATE_FUNCTIONS:
                        aggregate_columns.append(item)
                    elif list(item['value'].keys())[0].lower() in OPERATOR_PLUGINS:
                        operate_plugins.append(item)
                    else:
                        scalar_columns.append(item)
                else:
                    raise ValueError

            # if there is no groupby clause in sql, most one of following list not empty
            if aggregate_columns:
                value = '| summarize {0}'.format(self.dispatch(select)).replace('"', '')  # summarize not need escape

            if scalar_columns:
                value = '| project {0}'.format(self.dispatch(select))

            if operate_plugins:
                value = '| evaluate {0}'.format(self.dispatch(select))

            return value

