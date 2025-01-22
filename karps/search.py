from karps.config import Config, get_resource_config
from karps.database import run_queries
from karps.models import LexiconResult, SearchResult


def search(config: Config, resources: list[str], q: str | None = None) -> SearchResult:
    if q:
        [field, value] = q.split("|")[1:]
        where_clause = f"WHERE {field} = '{value}'"
    else:
        where_clause = ""

    sql_queries = [f"SELECT * FROM {resource} {where_clause}" for resource in resources]
    results = zip(resources, run_queries(config, sql_queries))

    total = 0
    lexicon_results = {}
    for resource, hits in results:
        rc = get_resource_config(resource)
        hits = [{"entry": rc.format_hit(hit) for hit in hits}]
        lexicon_total = len(hits)
        lexicon_results[resource] = LexiconResult(hits=hits, total=lexicon_total)
        total += lexicon_total

    return SearchResult(hits=lexicon_results, total=total)
