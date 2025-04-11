from contextlib import contextmanager
import re
from typing import Iterable, Iterator
import mysql.connector
from mysql.connector.connection import MySQLConnection

from karps.config import Config
from karps.query.query import Query, as_sql


def get_connection(config: Config) -> MySQLConnection:
    return mysql.connector.connect(
        host=config.host,
        user=config.user,
        password=config.password,
        database=config.database,
    )


def get_search(resources: list[str], q: Query | None, selection: Iterable[str] = ("*")) -> list[str]:
    """
    For each resource, creates a select statement with a where clause with constraints from q
    """
    # don't send in resource_id here since it is not actually a column
    selection_str = ", ".join([col for col in selection if col != "resource_id"])

    def get_selection_str(resource_id):
        if "resource_id" in selection:
            return f"{selection_str}, '{resource_id}' as resource_id"
        return selection_str

    where_clause = as_sql(q)
    return [f"SELECT {get_selection_str(resource_id)} FROM `{resource_id}` {where_clause}" for resource_id in resources]


def add_size(s: list[str], size, _from):
    selection_match_regexp = re.compile("SELECT (.*) FROM")
    for search in s:
        selection_str = re.match(selection_match_regexp, search)[1]
        # return the original query with LIMIT + OFFSET and an additional query for counting totals
        yield search + f" LIMIT {size} OFFSET {_from}", search.replace(selection_str, "COUNT(*)")


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
def get_cursor(config: Config) -> object:
    connection = get_connection(config)
    try:
        cursor = connection.cursor()
        yield cursor
    finally:
        cursor.close()
        connection.close()


def run_paged_searches(config: Config, sql_queries: Iterable[Iterable[str]]) -> Iterator[list[tuple]]:
    with get_cursor(config) as cursor:
        for paged_query, count_query in sql_queries:
            cursor.execute(paged_query)
            columns = [desc[0] for desc in cursor.description]
            results = cursor.fetchall()
            cursor.execute(count_query)
            total = cursor.fetchall()[0][0]
            yield columns, results, total


def run_searches(config: Config, sql_queries: Iterable[str]) -> Iterator[list[tuple]]:
    with get_cursor(config) as cursor:
        for sql_query in sql_queries:
            cursor.execute(sql_query)
            columns = [desc[0] for desc in cursor.description]
            results = cursor.fetchall()
            yield columns, results
