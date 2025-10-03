from collections import defaultdict
from typing import Iterable, Sequence
from karps.config import Env, MainConfig, ResourceConfig, format_hit, ensure_fields_exist, get_collection_fields
from karps.database.database import add_aggregation, run_paged_searches, run_searches, get_search
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
    sort: Sequence[tuple[str, str]] = (),
) -> SearchResult:
    resources = sorted(resources, key=lambda r: alphanumeric_key(r.resource_id))
    used_resources, s = get_search(main_config, resources, parse_query(q), sort=sort)

    results, count_results = run_paged_searches(
        env, s, size=size, _from=_from, collection_fields=get_collection_fields(main_config, used_resources)
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


def count(
    env: Env,
    main_config: MainConfig,
    resources: list[ResourceConfig],
    q: str | None = None,
    compile: Sequence[str] = (),
    columns: Iterable[tuple[str, str]] = (),
    sort: Sequence[tuple[str, str]] = (),
) -> tuple[list[Header], list[list[object]]]:
    compile = sorted(compile, key=alphanumeric_key)
    # sort columns by the "exploding" column
    columns = sorted(columns, key=lambda column: alphanumeric_key(column[0]))
    flattened_columns = list(set([item for sublist in columns or () for item in sublist if item != "_count"]))
    selection = compile + flattened_columns
    ensure_fields_exist(resources, selection)
    _, s = get_search(main_config, resources, parse_query(q), selection=selection, sort=[])
    agg_s = add_aggregation(s, compile=compile, columns=flattened_columns, sort=sort)

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
                    column_identifier = (col_name, col_val, elem[col_name])
                    if col_val != "_count":
                        if column_identifier not in entry_data:
                            entry_data[column_identifier] = set()
                        if elem[col_val] is not None:
                            if isinstance(elem[col_val], list):
                                add_elem = tuple(elem[col_val])
                            else:
                                add_elem = elem[col_val]
                            entry_data[column_identifier].add(add_elem)
                    else:
                        if column_identifier not in entry_data:
                            entry_data[column_identifier] = 0
                        entry_data[column_identifier] += 1
                    entry_headers[col_name, col_val].add(elem[col_name])
        result.append((tmp_row, entry_data, total))

    entry_header2: list[ValueHeader] = []
    for (explode_field, col_val), explode_values in entry_headers.items():
        for explode_value in explode_values:
            if col_val != "_count":
                header = ValueHeader(
                    type="value", header_value=explode_value, header_field=explode_field, column_field=col_val
                )
            else:
                header = ValueHeader(type="total", header_value=explode_value, header_field=explode_field)
            entry_header2.append(header)

    def columns_key(x: ValueHeader):
        if not x.column_field:
            column = "count"
        else:
            column = x.column_field
        return alphanumeric_key(x.header_field), alphanumeric_key(column), alphanumeric_key(x.header_value)

    # add the column headers for extra columns
    final_headers.extend(sorted(entry_header2, key=columns_key))
    # add the column header for "total"
    final_headers.append(Header(type="total"))
    rows = []
    for tmp_row, entry_data, total in result:
        for entry_header in entry_header2:
            if entry_header.column_field:
                column = entry_header.column_field
            else:
                column = "_count"
            asdf = entry_data.get((entry_header.header_field, column, entry_header.header_value), [])
            if column != "_count":
                tmp_row.append(list(asdf))
            else:
                tmp_row.append(asdf)
        tmp_row.append(total)
        rows.append(tmp_row)

    return final_headers, rows
