from collections import defaultdict
from typing import Iterable, Sequence
from karps.config import (
    Env,
    MainConfig,
    ResourceConfig,
    format_hit,
    ensure_fields_exist,
    get_collection_fields,
    get_table_fields,
)
from karps.database.database import (
    add_aggregation,
    run_paged_searches,
    run_searches,
    get_search,
)
from karps.database.query import SQLQuery
from karps.errors.errors import InternalError, UserError
from karps.models import CountRequest, Header, HitResponse, SearchResult, ValueHeader
from karps.query.query import parse_query
from karps.util.sorting import alphanumeric_key


def search(
    env: Env,
    main_config: MainConfig,
    resources: list[ResourceConfig],
    q: str | None = None,
    size: int = 10,
    _from: int = 0,
    sort: Sequence[tuple[str, str]] = (),
) -> SearchResult:
    resources = sorted(resources, key=lambda r: alphanumeric_key(r.resource_id))
    used_resources, s = get_search(main_config, resources, parse_query(q), sort=sort)

    results, count_results = run_paged_searches(
        env,
        s,
        size=size,
        _from=_from,
        collection_fields=get_collection_fields(main_config, used_resources),
        table_fields=get_table_fields(main_config, used_resources),
    )

    total = 0
    all_hits = []
    resource_hits = {}
    resource_order = []
    page_exists = _from == 0
    for resource_config, resource_hit in zip(used_resources, results):
        if resource_hit is None:
            continue
        page_exists = True
        (_, hits) = resource_hit
        hits = [
            HitResponse(
                **{
                    "entry": format_hit(main_config, resource_config, hit),
                    "resource_id": resource_config.resource_id,
                }
            )
            for hit in hits
        ]

        all_hits.extend(hits)
        if len(all_hits) > size:
            break

    if not page_exists:
        raise UserError(f"Requested from does not exist, value: {_from}")

    for resource_config, lexicon_total in zip(used_resources, count_results):
        resource_order.append(resource_config.resource_id)
        if lexicon_total is None:
            raise InternalError("Count queries failed")
        resource_hits[resource_config.resource_id] = lexicon_total
        total += lexicon_total

    return SearchResult(hits=all_hits, resource_hits=resource_hits, resource_order=resource_order, total=total)


def _make_column_data(data_column, column, entry_headers):
    """
    Find out which potentially new headers this row will create and layout data
    """
    col_field = column[0]
    cell_field = column[1]
    entry_data = {}
    for elem in data_column:
        col_val = elem[col_field]
        if cell_field != "_count":
            cell_val = elem[cell_field]
        else:
            cell_val = ()
        column_identifier = (col_field, col_val, cell_field)
        entry_data[column_identifier] = {"values": cell_val, "count": elem["count"]}
        entry_headers[col_field, cell_field].add(col_val)
    return entry_data


def _columns_key(x: ValueHeader):
    if not x.column_field:
        column = "count"
    else:
        column = x.column_field
    return alphanumeric_key(x.header_field), alphanumeric_key(column), alphanumeric_key(x.header_value)


def _create_columns_headers(columns_headers):
    headers: list[ValueHeader] = []
    for (explode_field, col_val), explode_values in columns_headers.items():
        for explode_value in explode_values:
            if col_val != "_count":
                header = ValueHeader(
                    type="value", header_value=explode_value, header_field=explode_field, column_field=col_val
                )
            else:
                header = ValueHeader(type="count", header_value=explode_value, header_field=explode_field)
            headers.append(header)
    return sorted(headers, key=_columns_key)


def count(
    env: Env,
    main_config: MainConfig,
    resources: list[ResourceConfig],
    q: str | None = None,
    compile: Sequence[str] = (),
    columns: Iterable[tuple[str, str]] = (),
    sort: Sequence[tuple[str, str]] = (),
) -> tuple[list[Header], list[list[object]], list[object]]:
    compile = sorted(compile, key=alphanumeric_key)
    # sort columns by the "exploding" column
    columns = sorted(columns, key=lambda column: alphanumeric_key(column[0]))

    # just the fields used in compile here
    final_headers = [Header(type="compile", column_field=header) for header in compile]
    # add the column header for "total"
    final_headers.append(Header(type="total"))

    query = parse_query(q)
    rows = []
    for column in columns:
        model_headers = _count_subquery(main_config, env, resources, query, compile, column, sort, rows)
        # add the column headers for extra columns
        final_headers.extend(model_headers)
    total_row = []
    _count_subquery(main_config, env, resources, query, [], ("resource_id", "_count"), None, total_row)

    return final_headers, rows, ["-"] + total_row[0]


def _count_subquery(main_config, env, resources, query, compile, column, sort, rows):
    selection = set(compile + ([column[0]] + ([column[1]] if column[1] != "_count" else []) if column else []))
    ensure_fields_exist(resources, selection)
    configs, s = get_search(main_config, resources, query, selection=selection, sort=[])
    s2: Sequence[tuple[ResourceConfig, SQLQuery]] = list(zip(configs, s))

    agg_s = add_aggregation(s2, compile, column, sort=sort)

    _, res = next(
        run_searches(
            env,
            [agg_s],
            CountRequest(compile=compile, columns=column),
            collection_fields=get_collection_fields(main_config, resources),
            table_fields=get_table_fields(main_config, resources),
        )
    )

    # collect the headers caused by using columns-parameter (not known at query time)
    columns_headers = defaultdict(set)

    def handle_row(row):
        if column:
            entry_data = _make_column_data(row[-1], column, columns_headers)
        else:
            entry_data = {}
        # append total directly after compile columns
        return list(row[1:-1]) + [int(row[0])], entry_data

    result = []
    for row in res:
        result.append(handle_row(row))

    # add the column headers for extra columns
    model_headers = _create_columns_headers(columns_headers)

    append_to_existing = bool(rows)
    for i, (row, entry_data) in enumerate(result):
        if append_to_existing:
            use_row = []
        else:
            use_row = row
        for (explode_field, col_val), explode_values in columns_headers.items():
            for explode_value in sorted(explode_values, key=alphanumeric_key):
                cell_content = entry_data.get((explode_field, explode_value, col_val))
                if cell_content:
                    if column[1] == "_count":
                        use_row.append({"count": cell_content["count"]})
                    else:
                        # TODO sort values
                        use_row.append(
                            {
                                "count": cell_content["count"],
                                "values": [
                                    {"count": val["count"], "value": val[col_val]} for val in cell_content["values"]
                                ],
                            }
                        )
                else:
                    if column[1] == "_count":
                        use_row.append({"count": 0})
                    else:
                        use_row.append({"count": 0, "values": []})
        if append_to_existing:
            # for the succeding requests, append columns data to preexisting rows
            rows[i].extend(use_row)
        else:
            # for the first request, add rows
            rows.append(use_row)
    return model_headers
