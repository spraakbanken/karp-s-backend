from contextlib import contextmanager
import json
import re
from typing import Any, Iterable, Iterator, Sequence, cast
import mysql.connector
from mysql.connector.abstracts import MySQLConnectionAbstract
from mysql.connector.cursor import MySQLCursor
from mysql.connector.pooling import PooledMySQLConnection

from karps.config import Env, ResourceConfig
from karps.query.query import Query, as_sql
from karps.errors import errors


selection_match_regexp = re.compile("SELECT (.*) FROM")


def get_connection(config: Env) -> PooledMySQLConnection | MySQLConnectionAbstract:
    return mysql.connector.connect(
        host=config.host,
        user=config.user,
        password=config.password,
        database=config.database,
    )


def get_search(resources: list[ResourceConfig], q: Query | None, selection: Iterable[str] = ("*")) -> list[str]:
    """
    For each resource, creates a select statement with a where clause with constraints from q
    """
    # don't send in resource_id here since it is not actually a column
    selection_str = ", ".join([col for col in selection if col not in ["resource_id", "word"]])

    def get_selection_str(resource_config):
        res = [selection_str] if selection_str else []
        if "resource_id" in selection:
            res.append(f"'{resource_config.resource_id}' as resource_id")
        if "word" in selection:
            res.append(f"`{resource_config.word}` as word")
        return ", ".join(res)

    return [
        f"SELECT {get_selection_str(resource_config)} FROM `{resource_config.resource_id}` {as_sql(resource_config.word, q)}"
        for resource_config in resources
    ]


def add_size(s: str, size: int, _from: int) -> tuple[str, str]:
    """
    Takes an SQL query without LIMIT or OFFSET and creates a query for one page and for counting totals

    :param s: An SQL-query
    :param size: The number of hits to size the query to
    :param _from: Offset / the row to start from
    :return: A tuple consisting of one sized query and one query for totals
    """
    # extract the string between SELECT and FROM
    matches = re.match(selection_match_regexp, s)
    if not matches:
        raise errors.InternalError("Error adding page sizes")
    selection_str = matches[1]
    # return the original query with LIMIT + OFFSET and an additional query for counting totals
    return s + f" LIMIT {size} OFFSET {_from}", s.replace(selection_str, "COUNT(*)")


def add_aggregation(s: list[str], compile: Sequence[str], columns: list[str]) -> str:
    """
    Takes a string containing an list of SQL queries and does aggregations on
    the fields given in columns

    TODO if compile field is collection, the json_field must be expanded to as many rows as there are in the fields, use JSON_TABLE
    each result column and the field to be presented in it must be included in the inner rows
    """
    data_sql = "FROM (" + " UNION ALL ".join(s) + ") as innerq"
    groupby_sql = f"GROUP BY {', '.join(compile)}"
    compile_sql = ", " + ", ".join(compile) if compile else ""
    columns_sql = (
        ", "
        + f" CONCAT('[', GROUP_CONCAT(JSON_OBJECT({', '.join([f"'{column}', {column}" for column in columns])})), ']') as entry_data"
        if columns
        else ""
    )
    select_sql = "SELECT count(*) as total" + compile_sql + columns_sql

    # TODO add sort by *all* the compile parameters
    sort_sql = f"ORDER BY {compile[0]}"
    res = " ".join((select_sql, data_sql, groupby_sql, sort_sql))
    return res


@contextmanager
def get_cursor(config: Env) -> Iterator[MySQLCursor]:
    connection = get_connection(config)
    cursor = None
    try:
        # When connection.cursor is called without arguments, a MySQLCursor-instance is returned
        #   Explicitly casting improves type hints from cursor-methods such as fetchall
        cursor = cast(MySQLCursor, connection.cursor())
        yield cursor
    finally:
        if cursor:
            cursor.close()
        connection.close()


def fetchall(cursor: MySQLCursor, sql: str) -> tuple[list[str], list[tuple]]:
    cursor.execute(sql)
    columns = [desc[0] for desc in cursor.description or ()]
    return columns, cursor.fetchall()


def run_searches(
    config: Env, sql_queries: Iterable[str], json_fields: Sequence = ()
) -> Iterator[tuple[list[str], list[list[Any]]]]:
    for columns, result, _ in run_paged_searches(config, sql_queries, paged=False, json_fields=json_fields):
        yield columns, result


def run_paged_searches(
    config: Env,
    in_sql_queries: Iterable[str],
    size: int = 10,
    _from: int = 0,
    paged=True,
    json_fields: Sequence = (),
) -> list[tuple[list[str], list[list[Any]], int | None]]:
    if paged:
        sql_queries = [add_size(s, size, _from) for s in in_sql_queries]
    else:
        sql_queries = [(s, None) for s in in_sql_queries]
    res: list[tuple] = []
    with get_cursor(config) as cursor:
        for sql_query, count_query in sql_queries:
            columns, result = fetchall(cursor, sql_query)
            if count_query:
                _, count_result = fetchall(cursor, count_query)
                total = count_result[0][0]
            else:
                total = None

            # TODO do this in lazily
            new_result = []
            for row in result:
                new_row = []
                for i, column in enumerate(columns):
                    if column == "entry_data":
                        entries_data = json.loads(str(row[i]))
                        if json_fields:
                            for entry_data in entries_data:
                                for json_field in json_fields:
                                    if json_field in entry_data:
                                        # MariaDB 10.6.21 autoparses JSON, MariaDB 10.3.39 does not, we need to handle both
                                        if not isinstance(entry_data[json_field], list):
                                            entry_data[json_field] = json.loads(entry_data[json_field])
                        new_row.append(entries_data)
                    else:
                        new_row.append(row[i])
                new_result.append(new_row)
            res.append((columns, new_result, total))
    return res
