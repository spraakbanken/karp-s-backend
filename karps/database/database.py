from contextlib import contextmanager
import json
import sys
from typing import Any, Iterable, Iterator, Sequence, cast
import mysql.connector
from mysql.connector.abstracts import MySQLConnectionAbstract
from mysql.connector.cursor import MySQLCursor
from mysql.connector.pooling import PooledMySQLConnection

from karps.config import Env, MainConfig, ResourceConfig
from karps.logging import get_sql_logger
from karps.query.query import Query, get_query
from karps.database.query import SQLQuery, select


def get_connection(config: Env) -> PooledMySQLConnection | MySQLConnectionAbstract:
    return mysql.connector.connect(
        host=config.host,
        user=config.user,
        password=config.password,
        database=config.database,
    )


sql_logger = get_sql_logger()


@contextmanager
def get_cursor(config: Env) -> Iterator[MySQLCursor]:
    connection = get_connection(config)
    cursor = None
    try:
        # When connection.cursor is called without arguments, a MySQLCursor-instance is returned
        #   Explicitly casting improloggves type hints from cursor-methods such as fetchall
        cursor = cast(MySQLCursor, connection.cursor())
        yield cursor
    finally:
        if cursor:
            cursor.close()
        connection.close()


def fetchall(cursor: MySQLCursor, sql: str) -> tuple[list[str], list[tuple]]:
    try:
        cursor.execute(sql)
    finally:
        sql_logger.info(sql, exc_info=sys.exc_info()[0])  # pyright: ignore[reportArgumentType]
    columns = [desc[0] for desc in cursor.description or ()]
    return columns, cursor.fetchall()


def get_search(
    main_config: MainConfig, resources: list[ResourceConfig], q: Query | None, selection: Iterable[str] = ("*")
) -> list[SQLQuery]:
    """
    For each resource, creates a select statement with a where clause with constraints from q
    """

    fields = main_config.fields

    selection_str: list[tuple[str, str | None]] | None = None
    if "*" not in selection:
        # don't send in resource_id here since it is not actually a column
        selection_str = [(col, None) for col in selection if col not in ["resource_id", "entry_word"]]

    def get_selection_str(
        resource_config, selection_str: list[tuple[str, str | None]] | None
    ) -> list[tuple[str, str | None]]:
        if "*" in selection:
            selection_str = [(field, None) for field in resource_config.fields]

        if not selection_str:
            sel = []
        else:
            sel = list(selection_str)
        if "resource_id" in selection:
            sel.append((f"'{resource_config.resource_id}'", "resource_id"))
        if "entry_word" in selection:
            sel.append((resource_config.entry_word.field, "entry_word"))
        return sel

    res = []
    for resource_config in resources:
        sel = get_selection_str(resource_config, selection_str)
        sql_q = select(sel).from_table(resource_config.resource_id)

        # get sql where clause from query
        where_field, where = get_query(main_config, resource_config.entry_word.field, q)

        for field in resource_config.fields:
            # only join tables that are used in selection
            # TODO must also add joins that are used in queries
            if fields[field].collection:
                where_kwarg = {}
                if where and where_field == field:
                    # add where clause to inner/cte/join-query, always called "value"
                    where_kwarg = {"where": where}
                if field in [s[0] for s in sel] or where_kwarg:
                    aliases = [alias for col, alias in sel if col == field]
                    sql_q.join(field, **where_kwarg, alias=aliases[0] if aliases else None)

        if where and where_field and not fields[where_field].collection:
            # add where clause to outer query
            sql_q.where(where)

        res.append(sql_q)
    return res


def add_aggregation(queries: list[SQLQuery], compile: Sequence[str], columns: list[str]) -> SQLQuery:
    """
    Takes a string containing an list of SQL queries and does aggregations on
    the fields given in columns

    TODO if compile field is collection, the json_field must be expanded to as many rows as there are in the fields, use JSON_TABLE
    each result column and the field to be presented in it must be included in the inner rows
    """
    sel: list[tuple[str, str | None]] = [("COUNT(*)", "total")]
    for c in compile:
        sel.append((c, None))
    if columns:
        sel.append(
            (
                # TODO move all sql generation into karps.database.query
                f"CONCAT('[', GROUP_CONCAT(JSON_OBJECT({', '.join([f"'{column}', {column}" for column in columns])})), ']')",
                "entry_data",
            )
        )

    s = select(sel).from_inner_query(queries)
    s.group_by(", ".join(compile))
    # TODO add sort by *all* the compile parameters
    s.order_by(compile[0])
    return s


def run_searches(
    config: Env, sql_queries: Iterable[SQLQuery], collection_fields: Iterable = ()
) -> Iterator[tuple[list[str], list[list[Any]]]]:
    results, _ = run_paged_searches(config, sql_queries, paged=False, collection_fields=collection_fields)
    for columns, result in results:
        yield columns, result


def run_paged_searches(
    config: Env,
    in_sql_queries: Iterable[SQLQuery],
    size: int = 10,
    _from: int = 0,
    paged=True,
    collection_fields: Iterable = (),
) -> tuple[Iterable[tuple[list[str], list[list[Any]]] | None], list[int]]:
    sql_queries = [s.to_string(paged=paged) for s in in_sql_queries]

    # fetch the total counts for each resource/query
    count_res: list[int] = []
    with get_cursor(config) as cursor:
        for _, count_query in sql_queries:
            if count_query:
                _, count_result = fetchall(cursor, count_query)
                count_res.append(count_result[0][0])

    # if the query uses paging, be must add the limits from user supplied _from and size
    # but also count_res, which contain the number of hits in each resource
    sql_queries_updated = []
    if paged:
        row_count = 0
        total_count = 0
        query_from = _from
        # use count_res to know which queries to execute
        for count, in_sql_query in zip(count_res, in_sql_queries):
            total_count += count
            # the number of rows to get from this query is min of available rows or needed rows
            query_size = min(total_count - query_from, count, max(0, size - row_count))
            if query_size > 0:
                if query_from != 0:
                    # adapt query_from to current resource
                    query_from = count - (total_count - query_from)
                # I think from_page is an incorrect name and from_entry/row is correct
                sql_queries_updated.append(
                    in_sql_query.from_page(query_from).add_size(query_size).to_string(paged=True)
                )
                row_count += query_size
                # only the first executed query need to have from != 0
                query_from = 0
            else:
                # when found is size, we don't need to do more queries, append empty placeholder for now
                sql_queries_updated.append(None)
    else:
        sql_queries_updated = sql_queries

    def res():
        # a generator to avoid fetching any data we do not need
        for resource_query in sql_queries_updated:
            if resource_query is None:
                # yield empty placeholder
                yield None
            else:
                sql_query = resource_query[0]
                with get_cursor(config) as cursor:
                    columns, result = fetchall(cursor, sql_query)
                new_result = []
                for row in result:
                    new_row = []
                    for i, column in enumerate(columns):
                        if column == "entry_data":
                            # for statistics, data shown but not used in compile are returned in a column
                            # called "entry_data", in JSON format. in the JSON, list fields are represented as
                            # \u001f separated values
                            entries_data = json.loads(str(row[i]))
                            if collection_fields:
                                for entry_data in entries_data:
                                    for json_field in collection_fields:
                                        if json_field in entry_data:
                                            entry_data[json_field] = (
                                                entry_data[json_field].split("\u001f") if entry_data[json_field] else []
                                            )
                            new_row.append(entries_data)
                        elif column in collection_fields:
                            new_row.append(row[i].split("\u001f") if row[i] else [])
                        else:
                            new_row.append(row[i])
                    new_result.append(new_row)
                yield (columns, new_result)

    return res(), count_res
