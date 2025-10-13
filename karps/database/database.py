from contextlib import contextmanager
import json
import sys
import time
from typing import Any, Iterable, Iterator, Sequence, cast
import mysql.connector
from mysql.connector.abstracts import MySQLConnectionAbstract
from mysql.connector.cursor import MySQLCursor
from mysql.connector.pooling import PooledMySQLConnection

from karps.config import Env, MainConfig, ResourceConfig
from karps.errors.errors import UserError
from karps.logging import get_sql_logger
from karps.models import CountRequest, Request
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
    bf = time.time()
    try:
        cursor.execute(sql)
    finally:
        af = time.time()
        sql_logger.info("", {"q": sql, "took": af - bf}, exc_info=sys.exc_info()[0])  # pyright: ignore[reportArgumentType]
    columns = [desc[0] for desc in cursor.description or ()]
    return columns, cursor.fetchall()


def _check_sort_allowed(resource_config, sort):
    """
    Raise if any field name used in sort is not available in the given resource
    """
    for field, _ in sort:
        if field not in resource_config.fields:
            raise UserError(f'Sort by "{field}" is not supported in "{resource_config.resource_id}"')


def _get_data_selection(resource_config: ResourceConfig, selection: Iterable[str]) -> list[tuple[str, str | None]]:
    """
    Takes a resource configuration and the requested selection and returns the selection to be used
    along with any needed aliases. If selection is ["*"] all fields in resource will be selected
    """
    sel: list[tuple[str, str | None]] | None = None
    if "*" in selection:
        sel = [(field, None) for field in resource_config.fields]
    else:
        # don't send in resource_id here since it is not actually a column
        sel = [(col, None) for col in selection if col not in ["resource_id", "entry_word"]]

    if "resource_id" in selection:
        sel.append((f"'{resource_config.resource_id}'", "resource_id"))
    if "entry_word" in selection:
        sel.append((resource_config.entry_word.field, "entry_word"))
    return sel


def get_search(
    main_config: MainConfig,
    resources: list[ResourceConfig],
    q: Query,
    selection: Iterable[str] = ("*"),
    sort: Sequence[tuple[str, str]] = (),
) -> tuple[list[ResourceConfig], list[SQLQuery]]:
    """
    For each resource, creates a select statement with a where clause with constraints from q
    Returns a tuple of resource IDs and corresponding queries, because it is possble that
    not all requested resources are supported for the search.
    """

    fields = main_config.fields

    res_resources = []
    res_q = []
    for resource_config in resources:
        sel = _get_data_selection(resource_config, selection)
        sql_q = select(sel).from_table(resource_config.resource_id)

        # get sql where clause from query
        bool_op, parts = get_query(main_config, resource_config.entry_word.field, q)

        ignore_resource = False
        for field, _ in parts:
            if field not in resource_config.fields:
                # if a query is posed with a field that is not supported in the resource, ignore the resource
                ignore_resource = True
                break
        if ignore_resource:
            continue

        for field in resource_config.fields:
            if parts:
                sql_q.op(bool_op)
            # only join tables that are used in selection
            # TODO must also add joins that are used in queries
            if fields[field].collection:
                where_kwarg = {}
                for where_field, where in parts:
                    if where and where_field == field:
                        # add where clause to inner/cte/join-query, always called "value"
                        where_kwarg = {"where": where}
                if field in [s[0] for s in sel] or where_kwarg:
                    aliases = [alias for col, alias in sel if col == field]
                    sql_q.join(field, **where_kwarg, alias=aliases[0] if aliases else None)
            for where_field, where in parts:
                if where and where_field and where_field == field and not fields[where_field].collection:
                    # add where clause to outer query
                    sql_q.where(where)
        if sort:
            if sort[0][0] == "_default":
                order = sort[0][1]
                # use the resource's default field
                sql_q.order_by([(resource_config.entry_word.field, order)])
            else:
                # check that the sort fields are available in resource
                _check_sort_allowed(resource_config, sort)
                sql_q.order_by(sort)
        res_resources.append(resource_config)
        res_q.append(sql_q)
    return res_resources, res_q


def add_aggregation(
    queries: Sequence[tuple[ResourceConfig | None, SQLQuery]],
    compile: Sequence[str],
    column: tuple[str, str],
    sort: Sequence[tuple[str, str]] = (),
):
    def inner(
        queries: Sequence[tuple[ResourceConfig | None, SQLQuery]],
        compile: Sequence[str],
        collect: Sequence[str] = [],
        sort: Sequence[tuple[str, str]] = (),
        innermost=False,
    ) -> SQLQuery:
        """
        Takes a string containing an list of SQL queries and does aggregations on
        the fields given in columns

        TODO if compile field is collection, the json_field must be expanded to as many rows as there are in the fields, use JSON_TABLE
        each result column and the field to be presented in it must be included in the inner rows
        """
        if compile[-1] == "_count":
            sel: list[tuple[str, str | None]] = []
        elif innermost:
            sel = [("COUNT(*)", "count")]
        else:
            sel = [("SUM(count)", "count")]
        for c in compile:
            if c != "_count":
                sel.append((c, None))
        # TODO handle different sizes of collect
        for field in collect[0:1]:
            inner_fields = []
            if len(collect) > 1:
                for tmpfield in collect[1:]:
                    if tmpfield != "_count":
                        inner_fields.append(f"'{tmpfield}', `{tmpfield}`")
            if field != "_count":
                sel.append(
                    (
                        # TODO move all sql generation into karps.database.query
                        f"CONCAT('[', GROUP_CONCAT(JSON_OBJECT('{field}', `{field}`,'count', `count`{',' + ','.join(inner_fields) if inner_fields else ''})), ']')",
                        f"`{field}`",
                    )
                )

        s = select(sel).from_inner_query(queries)
        if compile[-1] != "_count":
            s.group_by(compile)
        return s

    agg_s = None
    # columns[0][1] could be "_count", which just does _count on columns[0][0]
    # first aggregation
    agg_s = inner(queries, compile=list(compile) + list(column), innermost=True)
    # second level, this will be used for data columns
    agg_s = inner(
        [(None, agg_s)],
        compile=(list(compile) + [column[0]]),
        collect=[column[1]],
        innermost=column[1] == "_count",
    )
    # final level, adds sorting
    s = inner([(None, agg_s)], compile=compile, collect=column, sort=[])  # TODO sort

    if not sort or sort[0][0] == "_default":
        order = sort[0][1] if sort else "asc"
        sorts = [(field, order) for field in compile]
        s.order_by(sorts)
    else:
        for field, _ in sort:
            if field not in compile:
                raise UserError(f'Sort by "{field}" is not supported in with compile: {", ".join(compile)}')
        s.order_by(sort)
    return s


def run_searches(
    config: Env,
    sql_queries: Iterable[SQLQuery],
    request: CountRequest,
    collection_fields: Iterable = (),
) -> Iterator[tuple[list[str], list[list[Any]]]]:
    results, _ = run_paged_searches(
        config, sql_queries, paged=False, collection_fields=collection_fields, request=request
    )
    for columns, result in results:
        yield columns, result


def run_paged_searches(
    config: Env,
    in_sql_queries: Iterable[SQLQuery],
    size: int = 10,
    _from: int = 0,
    paged=True,
    collection_fields: Iterable = (),
    request: Request = Request(),
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
                    result_columns, result = fetchall(cursor, sql_query)
                new_result = []
                for row in result:
                    new_row = []
                    for i, column in enumerate(result_columns):
                        if isinstance(request, CountRequest) and i > len(request.compile):
                            # for statistics, data shown but not used in compile are returned in a column
                            # in JSON format. in the JSON, there are counts for each level and possibly values
                            try:
                                entries_data = json.loads(str(row[i]))
                            except json.decoder.JSONDecodeError:
                                raise UserError(
                                    f"Unable to process data, probably due to too many values per row, using {','.join(['='.join(a) for a in request.columns])}"
                                )
                            for elem in entries_data:
                                for key in elem:
                                    if key not in [column, "count"]:
                                        elem[key] = json.loads(str(elem[key]))
                                        #  elem[key] is a list. Each element in elem[key] is
                                        # an object with keys <field> and count, if <field> is a collection,
                                        # the value must be separated
                                        field = request.columns[1]
                                        if field in collection_fields:
                                            for x in elem[key]:
                                                if x[field]:
                                                    x[field] = x[field].split("\u001f")
                                                else:
                                                    x[field] = []
                            new_row.append(entries_data)
                        elif column == "count":
                            new_row.append(int(row[i]))
                        elif column in collection_fields:
                            new_row.append(row[i].split("\u001f") if row[i] else [])
                        else:
                            new_row.append(row[i])
                    new_result.append(new_row)
                yield (result_columns, new_result)

    return res(), count_res
