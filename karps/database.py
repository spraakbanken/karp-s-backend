from contextlib import contextmanager
import re
from typing import Iterable, Iterator
import mysql.connector
from mysql.connector.connection import MySQLConnection

from karps.config import Env, ResourceConfig
from karps.query.query import Query, as_sql


selection_match_regexp = re.compile("SELECT (.*) FROM")


def get_connection(config: Env) -> MySQLConnection:
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
    selection_str = re.match(selection_match_regexp, s)[1]
    # return the original query with LIMIT + OFFSET and an additional query for counting totals
    return s + f" LIMIT {size} OFFSET {_from}", s.replace(selection_str, "COUNT(*)")


def add_aggregation(s: list[str], compile: list[str], columns: list[str]) -> str:
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
    s = " ".join((select_sql, data_sql, groupby_sql, sort_sql))
    return s


@contextmanager
def get_cursor(config: Env) -> object:
    connection = get_connection(config)
    try:
        cursor = connection.cursor()
        yield cursor
    finally:
        cursor.close()
        connection.close()


def fetchall(cursor, sql):
    cursor.execute(sql)
    columns = [desc[0] for desc in cursor.description]
    return columns, cursor.fetchall()


def run_searches(config: Env, sql_queries: Iterable[str]) -> Iterator[list[tuple]]:
    for columns, result, _ in run_paged_searches(config, sql_queries, paged=False):
        yield columns, result


def run_paged_searches(
    config: Env, sql_queries: Iterable[str], size: int = 10, _from: int = 0, paged=True
) -> Iterator[list[tuple]]:
    if paged:
        sql_queries = [add_size(s, size, _from) for s in sql_queries]
    else:
        sql_queries = [(s, None) for s in sql_queries]
    res = []
    with get_cursor(config) as cursor:
        for sql_query, count_query in sql_queries:
            columns, result = fetchall(cursor, sql_query)
            if count_query:
                _, count_result = fetchall(cursor, count_query)
                total = count_result[0][0]
            else:
                total = None
            res.append((columns, result, total))
    return res
