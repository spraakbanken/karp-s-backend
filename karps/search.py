import json
from karps.config import Config, get_resource_config
from karps.database import run_queries, get_sql
from karps.models import LexiconResult, SearchResult


def parse_query(q):
    if not q:
        return {}
    return dict(zip(["op", "field", "value"], q.split("|")))


def search(config: Config, resources: list[str], q: str | None = None) -> SearchResult:
    sql_queries = get_sql(resources, **parse_query(q))

    results = zip(resources, run_queries(config, sql_queries))

    total = 0
    lexicon_results = {}
    for resource, (_, hits) in results:
        rc = get_resource_config(resource)
        hits = [{"entry": rc.format_hit(hit)} for hit in hits]
        lexicon_total = len(hits)
        lexicon_results[resource] = LexiconResult(hits=hits, total=lexicon_total)
        total += lexicon_total

    return SearchResult(hits=lexicon_results, total=total)


def count(config: Config, resources: list[str], q: str | None = None, compile=(), columns=()) -> None:
    # TODO if compile field is collection, the json_field must be expanded to as many rows as there are in the fields, use JSON_TABLE
    # each result column and the field to be presented in it must be included in the inner rows
    flattened_columns = [item for sublist in columns for item in sublist]
    # don't send in resource_id here since it is not actually a column
    sql_queries = get_sql(
        resources,
        **parse_query(q),
        selection=", ".join(compile + [col for col in flattened_columns if col != "resource_id"]),
    )
    data_sql = "FROM (" + " UNION ALL ".join(sql_queries) + ") as innerq"
    groupby_sql = f"GROUP BY {', '.join(compile)}"
    compile_sql = ", " + ", ".join(compile) if compile else ""
    # TODO hard-coded fields
    columns_sql = (
        ", "
        + f"JSON_ARRAYAGG(JSON_OBJECT({', '.join([f"'{column}', {column}" for column in flattened_columns])})) as entry_data"
        if flattened_columns
        else ""
    )
    select_sql = "SELECT count(*) as total" + compile_sql + columns_sql

    # TODO add sort by all the compile parameters
    sort_sql = f"ORDER BY {compile[0]}"
    sql_query = " ".join((select_sql, data_sql, groupby_sql, sort_sql))

    result = []
    headers, res = next(run_queries(config, [sql_query]))
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
