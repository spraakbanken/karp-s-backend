from dataclasses import dataclass
import tatsu
import tatsu.exceptions
import importlib.resources

from karps.config import MainConfig
from karps.errors import errors

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

    op: str | None = None
    field: str | None = None
    value: object | None = None


def parse_query(q: str | None) -> Query | None:
    if q:
        try:
            ast = parser.parse(q)
        except tatsu.exceptions.FailedParse as e:
            raise errors.UserError("Parse error: " + e.message)
        if isinstance(ast.arg, list):
            arg = "".join(ast.arg)
        else:
            arg = ast.arg
        return Query(op=ast.op, field=ast.field, value=arg)
    else:
        return None


def get_epsilon(q_number):
    # for smaller floats we might need a smaller epsilon and vice versa for larger floats (magnitude, not precision)
    # this is work-in-progress and not fully tested
    return 0.01


def get_query(main_config: MainConfig, word_column: str, q: Query | None) -> tuple[str | None, str | None]:
    """
    Translates a query tree into an SQL WHERE clause.

    :param word_column: The column name to use when the query field is "entry_word"
    :param q: The root of the query tree. If None, returns an empty string.
    :return: A string representing the SQL WHERE clause.
    """
    if not (q and q.field):
        return None, None

    # If the field is "word", use the specified word_column, as it can differ across resources.
    if q.field == "entry_word":
        field = word_column
    else:
        field = q.field
    field_type: str = main_config.fields[field].type

    # collections are stored in sepearate tables where the column name is always value
    db_field = "value" if main_config.fields[field].collection else field

    if field_type == "float":
        if q.op == "equals":
            return field, f"ABS(`{field}` - {q.value}) < {get_epsilon(q.value)}"
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
        return field, f"`{db_field}` {op_arg}"
    else:
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
            raise errors.InternalError("unknown operator in query")
    return field, f"`{db_field}` {op_arg}"
