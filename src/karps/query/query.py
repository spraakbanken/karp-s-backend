from dataclasses import dataclass
from typing import Any, cast
import tatsu
import tatsu.exceptions
import importlib.resources

from karps.config import MainConfig
from karps.errors import errors

with importlib.resources.files("karps.query").joinpath("query.ebnf").open() as fp:
    grammar = fp.read()
    parser = tatsu.compile(grammar)


class Query: ...


class NullQuery(Query): ...


@dataclass
class SubQuery(Query):
    op: str
    field: str
    value: object | None = None


@dataclass
class LogicalQuery(Query):
    """
    Uses the same query language as Karp, described here:
    https://spraakbanken4.it.gu.se/karp/v7/#tag/Searching
    Sub-queries, freetext and multiple operands for `not(*)` is not supported
    """

    op: str
    clauses: list["Query"]


def parse_query(q: str | None) -> Query:
    if q:
        try:
            # TODO restore typing
            ast: Any = parser.parse(q)
        except tatsu.exceptions.FailedParse as e:
            raise errors.UserError("Parse error: " + e.message)

        def recurse(ast):
            if ast.op in ["and", "or", "not"]:
                queries = []
                for inner_ast in ast.args:
                    queries.append(recurse(inner_ast))
                if queries:
                    if ast.op == "not" and len(queries) > 1:
                        raise errors.UserError("Only one clause for not-operator supported")
                    return LogicalQuery(op=ast.op.upper(), clauses=queries)
                else:
                    return NullQuery()
            else:
                if isinstance(ast.arg, list):
                    arg = "".join(ast.arg)
                else:
                    arg = ast.arg
                return SubQuery(op=ast.op, field=ast.field, value=arg)

        return recurse(ast)
    else:
        return NullQuery()


def get_epsilon(q_number):
    # for smaller floats we might need a smaller epsilon and vice versa for larger floats (magnitude, not precision)
    # this is work-in-progress and not fully tested
    return 0.01


def get_query(
    main_config: MainConfig, word_column: str, outer_q: Query
) -> tuple[list[str], str | None, list[tuple[str, str]]]:
    """
    Translates a query tree into an SQL WHERE clause.

    :param word_column: The column name to use when the query field is entry_word / entryWord
    :param q: The root of the query tree. If None, returns an empty string.
    :return: A string representing the SQL WHERE clause.
    """
    if isinstance(outer_q, NullQuery):
        return [], None, []

    fields = []
    main_query: str
    # from collections
    collection_queries: list[tuple[str, str]] = []

    def recurse(q) -> tuple[str, bool]:
        if isinstance(q, LogicalQuery):
            parts = []
            for inner_q in q.clauses:
                a, complex = recurse(inner_q)
                if complex:
                    a = f"({a})"
                parts.append(a)
            if q.op == "NOT":
                return f"NOT {parts[0]}", True
            else:
                return f" {q.op} ".join(parts), True
        elif isinstance(q, SubQuery):
            # If the field is entry_word / entryWord, use the specified word_column, as it can differ across resources.
            if q.field in ["entry_word", "entryWord"]:
                field = word_column
            else:
                field = q.field
            fields.append(field)
            field_type: str = main_config.fields[field].type
            where_part = to_where_clause(field, field_type, q)
            if main_config.fields[field].collection:
                collection_queries.append((field, where_part))
                # TABLE_PREFIX will be replaced
                # {field} must have a counter
                where_part = f"EXISTS (SELECT 1 FROM `{field}__where` WHERE TABLE_PREFIX__id = __parent_id)"

            return where_part, False
        else:
            raise RuntimeError("cannot happen")

    main_query, _ = recurse(outer_q)

    return fields, main_query, collection_queries


def to_where_clause(field, field_type, q) -> str:
    if field_type == "float" or field_type == "integer":
        if q.op == "equals":
            return f"ABS(`{field}` - {q.value}) < {get_epsilon(q.value)}"
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
    return f"`{field}` {op_arg}"
