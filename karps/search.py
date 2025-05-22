from typing import Iterable, Sequence
from karps.config import Env, MainConfig, ResourceConfig, format_hit, ensure_fields_exist
from karps.database import add_aggregation, run_paged_searches, run_searches, get_search
from karps.models import HitResponse, LexiconResult, SearchResult
from karps.query.query import parse_query


def search(
    env: Env,
    main_config: MainConfig,
    resources: list[ResourceConfig],
    q: str | None = None,
    size: int = 10,
    _from: int = 0,
) -> SearchResult:
    s = get_search(resources, parse_query(q))

    results = zip(resources, run_paged_searches(env, s, size=size, _from=_from))

    total = 0
    lexicon_results = {}
    for resource_config, (_, hits, lexicon_total) in results:
        hits = [HitResponse(**{"entry": format_hit(main_config, resource_config, hit)}) for hit in hits]
        lexicon_results[resource_config.resource_id] = LexiconResult(hits=hits, total=lexicon_total)
        total += lexicon_total

    return SearchResult(hits=lexicon_results, total=total)


def count(
    config: Env,
    resources: list[ResourceConfig],
    q: str | None = None,
    compile: Sequence[str] = (),
    columns: Iterable[tuple[str, str]] = (),
) -> tuple[list[str], list[list[object]]]:
    flattened_columns = [item for sublist in columns or () for item in sublist]
    selection = set(list(compile) + flattened_columns)
    ensure_fields_exist(resources, selection)
    s = get_search(resources, parse_query(q), selection=selection)
    agg_s = add_aggregation(s, compile=compile, columns=flattened_columns)

    result = []
    headers, res = next(run_searches(config, [agg_s]))

    if flattened_columns:
        last_index = -1
    else:
        last_index = None

    tmp_headers = headers[1:last_index]
    entry_headers = set()

    for row in res:
        total = row[0]
        tmp_row = list(row[1:last_index])
        entry_data = {}
        if flattened_columns:
            for elem in row[-1]:
                for [col_name, col_val] in columns:
                    entry_data[elem[col_name]] = elem[col_val]
                    entry_headers.add(elem[col_name])
        result.append((tmp_row, entry_data, total))

    entry_headers = sorted(entry_headers)
    tmp_headers.extend(entry_headers)
    tmp_headers.append("total")
    rows = []
    for tmp_row, entry_data, total in result:
        for entry_header in entry_headers:
            tmp_row.append(entry_data.get(entry_header))
        tmp_row.append(total)
        rows.append(tmp_row)

    return tmp_headers, rows
