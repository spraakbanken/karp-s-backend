from dataclasses import dataclass
import tatsu
import importlib


with importlib.resources.files("karps.query").joinpath("query.ebnf").open() as fp:
    grammar = fp.read()
    parser = tatsu.compile(grammar)


@dataclass
class Query:
    """
    Uses the same query language as Karp, described here:
    https://spraakbanken4.it.gu.se/karp/v7/#tag/Searching
    Currently, support in this class in very limited
    """

    op: str = None
    field: str = None
    value: object = None


def parse_query(q: str | None) -> Query:
    if q:
        ast = parser.parse(q)
        if isinstance(ast.arg, list):
            arg = "".join(ast.arg)
        else:
            arg = ast.arg
        return Query(op=ast.op, field=ast.field, value=arg)
    else:
        return None


def as_sql(word_column: str, q: Query | None) -> str:
    """
    Translates a query tree into an SQL WHERE clause.

    :param word_column: The column name to use when the query field is "word"
    :param q: The root of the query tree. If None, returns an empty string.
    :return: A string representing the SQL WHERE clause.
    """
    if not q:
        return ""

    # If the field is "word", use the specified word_column, as it can differ across resources.
    if q.field == "word":
        field = word_column
    else:
        field = q.field

    if q.op == "equals":
        op_arg = f"= '{q.value}'"
    elif q.op == "startswith":
        op_arg = f"LIKE '{q.value}%'"
    elif q.op == "endswith":
        op_arg = f"LIKE '%{q.value}'"
    elif q.op == "contains":
        op_arg = f"LIKE '%{q.value}%'"
    elif q.op == "regexp":
        op_arg = f"REGEXP '{q.value}'"
    # TODO test these with integers
    elif q.op == "lt":
        op_arg = f"< '{q.value}'"
    elif q.op == "lte":
        op_arg = f"<= '{q.value}'"
    elif q.op == "gt":
        op_arg = f"> '{q.value}'"
    elif q.op == "gte":
        op_arg = f">= '{q.value}'"
    else:
        # this should not happen since the query parser would not accept other operators
        raise RuntimeError("unknown operator in query")
    where_clause = f"WHERE `{field}` {op_arg}"
    return where_clause
