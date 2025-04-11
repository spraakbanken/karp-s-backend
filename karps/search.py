import json
from typing import Iterable
from karps.config import Config, format_hit, get_resource_config
from karps.database import add_aggregation, add_size, run_paged_searches, run_searches, get_search
from karps.models import LexiconResult, SearchResult
from karps.query.query import parse_query


def search(
    config: Config, main_config, resources: list[str], q: str | None = None, size: int = 10, _from: int = 0
) -> SearchResult:
    s = get_search(resources, parse_query(q))

    sized_s = add_size(s, size, _from)

    results = zip(resources, run_paged_searches(config, sized_s))

    total = 0
    lexicon_results = {}
    for resource, (_, hits, total) in results:
        rc = get_resource_config(resource)
        hits = [{"entry": format_hit(main_config, rc, hit)} for hit in hits]
        lexicon_total = total
        lexicon_results[resource] = LexiconResult(hits=hits, total=lexicon_total)
        total += lexicon_total

    return SearchResult(hits=lexicon_results, total=total)


def count(
    config: Config, resources: list[str], q: str | None = None, compile: Iterable[str] = (), columns: Iterable[str] = ()
) -> tuple[list[str], list[list[object]]]:
    flattened_columns = [item for sublist in columns or () for item in sublist]
    s = get_search(resources, parse_query(q), selection=compile + flattened_columns)
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
            for elem in json.loads(row[-1]):
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
