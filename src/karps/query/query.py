from collections import defaultdict
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
                    # unescape string values
                    arg = arg.replace('\\"', '"')
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


# a tuple of str and a tuple[Any], str must contain as many %s as there are elements in the inner tuple
# used for parameterization of queries
type ReadyQuery = tuple[str, tuple[Any, ...]]


def get_query(
    main_config: MainConfig, word_column: str, outer_q: Query
) -> tuple[set[str], ReadyQuery | None, list[tuple[str, int, ReadyQuery]]]:
    """
    Translates a query tree into an SQL WHERE clause.

    :param word_column: The column name to use when the query field is entry_word / entryWord
    :param q: The root of the query tree. If None, returns an empty string.
    :return: A string representing the SQL WHERE clause.
    """
    if isinstance(outer_q, NullQuery):
        return set(), None, []

    fields = set()
    # from collections
    # TODO currently collections_queries are single clause which could be shown in the type system
    collection_queries: list[tuple[str, int, ReadyQuery]] = []

    collection_field_count = defaultdict(int)

    def recurse(q) -> tuple[ReadyQuery, bool]:
        """
        Returns a tuple of
        - the query tuple - a str and its params
        - a boolean used to know wether to wrap query in parentheses, only used inside recursion
        """
        if isinstance(q, LogicalQuery):
            parts = []
            params = []
            for inner_q in q.clauses:
                (a, inner_params), complex = recurse(inner_q)
                if complex:
                    a = f"({a})"
                parts.append(a)
                params.extend(inner_params)
            if q.op == "NOT":
                return (f"NOT {parts[0]}", tuple(params)), True
            else:
                return (f" {q.op} ".join(parts), tuple(params)), True
        elif isinstance(q, SubQuery):
            # If the field is entry_word / entryWord, use the specified word_column, as it can differ across resources.
            if q.field in ["entry_word", "entryWord"]:
                field = word_column
            else:
                field = q.field
            fields.add(field)
            field_type: str = main_config.fields[field].type
            where_part, params = to_where_clause(field, field_type, q)
            if main_config.fields[field].collection:
                count = collection_field_count[field]
                collection_queries.append((field, count, (where_part, (params,))))

                # TABLE_PREFIX will be replaced
                where_part = (
                    f"EXISTS (SELECT 1 FROM `{field}{f'_{count}'}__where` WHERE TABLE_PREFIX__id = __parent_id)"
                )
                params = ()
                collection_field_count[field] += 1
            else:
                params = (params,)

            return (where_part, params), False
        else:
            raise RuntimeError("cannot happen")

    main_query, _ = recurse(outer_q)

    return fields, main_query, collection_queries


def to_where_clause(field: str, field_type: str, q: SubQuery) -> tuple[str, Any]:
    if field_type == "float" or field_type == "integer":
        val = q.value
        if q.op == "equals":
            return f"ABS(`{field}` - %s) < {get_epsilon(q.value)}", q.value
        elif q.op == "lt":
            op_arg = f"< %s + {get_epsilon(q.value)}"
        elif q.op == "lte":
            op_arg = f"<= %s + {get_epsilon(q.value)}"
        elif q.op == "gt":
            op_arg = f"> %s - {get_epsilon(q.value)}"
        elif q.op == "gte":
            op_arg = f">= %s - {get_epsilon(q.value)}"
        else:
            raise errors.UserError("unsupported operator for numeric values")
        return f"`{field}` {op_arg}", val
    else:
        val = cast(str, q.value)

        # TODO need to escape values in the LIKE searches
        # val = val.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        if q.op == "equals":
            op = "="
        elif q.op == "startswith":
            op = "LIKE"
            val = f"{val}%"
        elif q.op == "endswith":
            op = "LIKE"
            val = f"%{val}"
        elif q.op == "contains":
            op = "LIKE"
            val = f"%{val}%"
        elif q.op == "regexp":
            op = "REGEXP"
        elif q.op == "lt":
            op = "<"
        elif q.op == "lte":
            op = "<="
        elif q.op == "gt":
            op = ">"
        elif q.op == "gte":
            op = ">="
        else:
            # this should not happen since the query parser would not accept other operators
            raise errors.InternalError("unknown operator in query")
        return f"`{field}` {op} %s", val
