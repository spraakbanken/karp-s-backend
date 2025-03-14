import json
from typing import Iterable
from karps.config import Config, get_resource_config
from karps.database import add_aggregation, add_size, run_paged_searches, run_searches, get_search
from karps.models import CountResult, LexiconResult, SearchResult
from karps.query import parse_query


def search(config: Config, resources: list[str], q: str | None = None, size: int = 10, _from: int = 0) -> SearchResult:
    s = get_search(resources, parse_query(q))

    sized_s = add_size(s, size, _from)

    results = zip(resources, run_paged_searches(config, sized_s))

    total = 0
    lexicon_results = {}
    for resource, (_, hits, total) in results:
        rc = get_resource_config(resource)
        hits = [{"entry": rc.format_hit(hit)} for hit in hits]
        lexicon_total = total
        lexicon_results[resource] = LexiconResult(hits=hits, total=lexicon_total)
        total += lexicon_total

    return SearchResult(hits=lexicon_results, total=total)


def count(
    config: Config, resources: list[str], q: str | None = None, compile: Iterable[str] = (), columns: Iterable[str] = ()
) -> CountResult:
    flattened_columns = [item for sublist in columns or () for item in sublist]
    s = get_search(resources, parse_query(q), selection=compile + flattened_columns)
    agg_s = add_aggregation(s, compile=compile, columns=flattened_columns)

    result = []
    headers, res = next(run_searches(config, [agg_s]))
    for row in res:
        total = row[0]
        if flattened_columns:
            last_index = -1
        else:
            last_index = None
        rest = {col: val for col, val in zip(headers[1:last_index], row[1:last_index])}
        rest["total"] = total
        if flattened_columns:
            for elem in json.loads(row[-1]):
                for [col_name, col_val] in columns:
                    rest[elem[col_name]] = elem[col_val]
        result.append(rest)
    return result
