from dataclasses import dataclass
from typing import cast
import tatsu
import tatsu.exceptions
import importlib.resources

from karps.config import MainConfig
from karps.errors import errors

with importlib.resources.files("karps.query").joinpath("query.ebnf").open() as fp:
    grammar = fp.read()
    parser = tatsu.compile(grammar)


@dataclass
class SubQuery:
    op: str
    field: str
    value: object | None = None


@dataclass
class Query:
    """
    Uses the same query language as Karp, described here:
    https://spraakbanken4.it.gu.se/karp/v7/#tag/Searching
    Currently, support in this class in very limited
    """

    op: str
    clauses: list[SubQuery]


def parse_query(q: str | None) -> Query:
    if q:
        try:
            ast = parser.parse(q)
        except tatsu.exceptions.FailedParse as e:
            raise errors.UserError("Parse error: " + e.message)
        query_parts = []
        for inner_ast in ast.args:
            if isinstance(inner_ast.arg, list):
                arg = "".join(inner_ast.arg)
            else:
                arg = inner_ast.arg
            query_parts.append(SubQuery(op=inner_ast.op, field=inner_ast.field, value=arg))
        return Query(op=ast.op, clauses=query_parts)
    else:
        return Query(op="and", clauses=[])


def get_epsilon(q_number):
    # for smaller floats we might need a smaller epsilon and vice versa for larger floats (magnitude, not precision)
    # this is work-in-progress and not fully tested
    return 0.01


def get_query(main_config: MainConfig, word_column: str, outer_q: Query) -> tuple[str | None, list[tuple[str, str]]]:
    """
    Translates a query tree into an SQL WHERE clause.

    :param word_column: The column name to use when the query field is entry_word / entryWord
    :param q: The root of the query tree. If None, returns an empty string.
    :return: A string representing the SQL WHERE clause.
    """
    if not outer_q.clauses:
        return None, []

    parts = []
    for q in outer_q.clauses:
        # If the field is entry_word / entryWord, use the specified word_column, as it can differ across resources.
        if q.field in ["entry_word", "entryWord"]:
            field = word_column
        else:
            field = q.field
        field_type: str = main_config.fields[field].type

        if field_type == "float" or field_type == "integer":
            if q.op == "equals":
                parts.append((field, f"ABS(`{field}` - {q.value}) < {get_epsilon(q.value)}"))
                continue
            elif q.op == "lt":
                op_arg = f"< {q.value} + {get_epsilon(q.value)}"
            elif q.op == "lte":
                op_arg = f"<= {q.value} + {get_epsilon(q.value)}"
            elif q.op == "gt":
                op_arg = f"> {q.value} - {get_epsilon(q.value)}"
            elif q.op == "gte":
                op_arg = f">= {q.value} - {get_epsilon(q.value)}"
            else:
                raise errors.UserError("unsupported operator for numeric values")
            parts.append((field, f"`{field}` {op_arg}"))
            continue
        else:
            # escape ' with a backslash, since we use ' for strings in MariaDB
            val = cast(str, q.value).replace("'", "\\'")
            if q.op == "equals":
                op_arg = f"= '{val}'"
            elif q.op == "startswith":
                op_arg = f"LIKE '{val}%'"
            elif q.op == "endswith":
                op_arg = f"LIKE '%{val}'"
            elif q.op == "contains":
                op_arg = f"LIKE '%{val}%'"
            elif q.op == "regexp":
                op_arg = f"REGEXP '{val}'"
            # TODO test these with integers
            elif q.op == "lt":
                op_arg = f"< '{val}'"
            elif q.op == "lte":
                op_arg = f"<= '{val}'"
            elif q.op == "gt":
                op_arg = f"> '{val}'"
            elif q.op == "gte":
                op_arg = f">= '{val}'"
            else:
                # this should not happen since the query parser would not accept other operators
                raise errors.InternalError("unknown operator in query")
        parts.append((field, f"`{field}` {op_arg}"))
    return outer_q.op, parts
