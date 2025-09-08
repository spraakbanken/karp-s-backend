from collections import defaultdict
from typing import Iterable, Sequence
from karps.config import Env, MainConfig, ResourceConfig, format_hit, ensure_fields_exist, get_collection_fields
from karps.database.database import add_aggregation, run_paged_searches, run_searches, get_search
from karps.database.query import SQLQuery
from karps.errors.errors import InternalError, UserError
from karps.models import Header, HitResponse, SearchResult, ValueHeader
from karps.query.query import parse_query
from karps.util import alphanumeric_key


def search(
    env: Env,
    main_config: MainConfig,
    resources: list[ResourceConfig],
    q: str | None = None,
    size: int = 10,
    _from: int = 0,
) -> SearchResult:
    resources = sorted(resources, key=lambda r: r.resource_id)
    s: list[SQLQuery] = get_search(main_config, resources, parse_query(q))

    results, count_results = run_paged_searches(
        env, s, size=size, _from=_from, collection_fields=get_collection_fields(main_config, resources)
    )

    total = 0
    all_hits = []
    resource_hits = {}
    resource_order = []
    page_exists = _from == 0
    for resource_config, resource_hit in zip(resources, results):
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

    for resource_config, lexicon_total in zip(resources, count_results):
        resource_order.append(resource_config.resource_id)
        if lexicon_total is None:
            raise InternalError("Count queries failed")
        resource_hits[resource_config.resource_id] = lexicon_total
        total += lexicon_total

    return SearchResult(hits=all_hits, resource_hits=resource_hits, resource_order=resource_order, total=total)


def count(
    env: Env,
    main_config: MainConfig,
    resources: list[ResourceConfig],
    q: str | None = None,
    compile: Sequence[str] = (),
    columns: Iterable[tuple[str, str]] = (),
) -> tuple[list[Header], list[list[object]]]:
    compile = sorted(compile, key=alphanumeric_key)
    # sort columns by the "exploding" column
    columns = sorted(columns, key=lambda column: alphanumeric_key(column[0]))
    flattened_columns = [item for sublist in columns or () for item in sublist]
    selection = set(list(compile) + flattened_columns)
    ensure_fields_exist(resources, selection)
    s = get_search(main_config, resources, parse_query(q), selection=selection)
    agg_s = add_aggregation(s, compile=compile, columns=flattened_columns)

    result = []
    headers, res = next(run_searches(env, [agg_s], collection_fields=get_collection_fields(main_config, resources)))

    if flattened_columns:
        last_index = -1
    else:
        last_index = None

    # just the fields used in compile here
    final_headers = [Header(type="compile", column_field=header) for header in headers[1:last_index]]
    entry_headers = defaultdict(set)

    for row in res:
        total = row[0]
        tmp_row = list(row[1:last_index])
        entry_data = {}
        if flattened_columns:
            for elem in row[-1]:
                for [col_name, col_val] in columns:
                    entry_data[elem[col_name]] = elem[col_val]
                    entry_headers[col_name, col_val].add(elem[col_name])
        result.append((tmp_row, entry_data, total))

    entry_header2: list[ValueHeader] = []
    for (explode_field, col_val), explode_values in entry_headers.items():
        for explode_value in explode_values:
            entry_header2.append(
                ValueHeader(type="value", header_value=explode_value, header_field=explode_field, column_field=col_val)
            )

    def columns_key(x: ValueHeader):
        if not x.column_field:
            raise InternalError()
        return alphanumeric_key(x.header_field), alphanumeric_key(x.column_field)

    # add the column headers for extra columns
    final_headers.extend(sorted(entry_header2, key=columns_key))
    # add the column header for "total"
    final_headers.append(Header(type="total"))
    rows = []
    for tmp_row, entry_data, total in result:
        for entry_header in entry_header2:
            tmp_row.append(entry_data.get(entry_header.header_value))
        tmp_row.append(total)
        rows.append(tmp_row)

    return final_headers, rows
