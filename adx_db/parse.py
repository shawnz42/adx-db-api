
import json
from threading import Lock
from collections import Mapping

from mo_future import binary_type, items, number_types, text
from pyparsing import ParseException, ParseResults
from adx_db.debugs import all_exceptions
from adx_db.sql_parser import SQLParser


parseLocker = Lock()  # ENSURE ONLY ONE PARSING AT A TIME


def parse(sql):
    with parseLocker:
        try:
            all_exceptions.clear()
            sql = sql.rstrip().rstrip(";")
            parse_result = SQLParser.parseString(sql, parseAll=True)
            return _scrub(parse_result)
        except Exception as e:
            if isinstance(e, ParseException) and e.msg == "Expected end of text":
                problems = all_exceptions.get(e.loc, [])
                expecting = [
                    f
                    for f in (set(p.msg.lstrip("Expected").strip() for p in problems)-{"Found unwanted token"})
                    if not f.startswith("{")
                ]
                raise ParseException(sql, e.loc, "Expecting one of (" + (", ".join(expecting)) + ")")
            raise


def _scrub(result):
    if isinstance(result, text):
        return result
    elif isinstance(result, binary_type):
        return result.decode('utf8')
    elif isinstance(result, number_types):
        return result
    elif not result:
        return {}
    elif isinstance(result, (list, ParseResults)):
        if not result:
            return None
        elif len(result) == 1:
            return _scrub(result[0])
        else:
            output = [
                rr
                for r in result
                for rr in [_scrub(r)]
                if rr is not None
            ]
            # IF ALL MEMBERS OF A LIST ARE LITERALS, THEN MAKE THE LIST LITERAL
            if all(isinstance(r, number_types) for r in output):
                pass
            elif all(isinstance(r, number_types) or (isinstance(r, Mapping) and "literal" in r.keys()) for r in output):
                output = {"literal": [r['literal'] if isinstance(r, Mapping) else r for r in output]}
            return output
    elif not items(result):
        return {}
    else:
        return {
            k: vv
            for k, v in result.items()
            for vv in [_scrub(v)]
            if vv is not None
        }


_ = json.dumps