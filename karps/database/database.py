from contextlib import contextmanager
import json

from typing import Any, Iterable, Iterator, Sequence, cast
import mysql.connector
from mysql.connector.abstracts import MySQLConnectionAbstract
from mysql.connector.cursor import MySQLCursor
from mysql.connector.pooling import PooledMySQLConnection

from karps.config import Env, MainConfig, ResourceConfig
from karps.query.query import Query, get_query
from karps.database.query import SQLQuery, select


def get_connection(config: Env) -> PooledMySQLConnection | MySQLConnectionAbstract:
    return mysql.connector.connect(
        host=config.host,
        user=config.user,
        password=config.password,
        database=config.database,
    )


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
        selection_str = [(col, None) for col in selection if col not in ["resource_id", "word"]]

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
        if "word" in selection:
            sel.append((resource_config.word, "word"))
        return sel

    res = []
    for resource_config in resources:
        sel = get_selection_str(resource_config, selection_str)
        sql_q = select(sel).from_table(resource_config.resource_id)

        # get sql where clause from query
        where_field, where = get_query(main_config, resource_config.word, q)

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

        if where and not fields[where_field].collection:
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
    config: Env, sql_queries: Iterable[SQLQuery], collection_fields: Iterable = ()
) -> Iterator[tuple[list[str], list[list[Any]]]]:
    for columns, result, _ in run_paged_searches(config, sql_queries, paged=False, collection_fields=collection_fields):
        yield columns, result


def run_paged_searches(
    config: Env,
    in_sql_queries: Iterable[SQLQuery],
    size: int = 10,
    _from: int = 0,
    paged=True,
    collection_fields: Iterable = (),
) -> list[tuple[list[str], list[list[Any]], int | None]]:
    if paged:
        sql_queries = [s.from_page(_from).add_size(size).to_string(paged=True) for s in in_sql_queries]
    else:
        sql_queries = [s.to_string() for s in in_sql_queries]
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
            res.append((columns, new_result, total))
    return res
