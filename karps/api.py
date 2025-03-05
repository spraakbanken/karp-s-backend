from fastapi import FastAPI, Depends, Query

from karps.config import Config, ResourceConfig, get_config, get_resource_configs
from karps.search import search, count
from karps.models import CountResult, SearchResult

api_description = """
Språkbanken has many lexical resources, listed on our [webpage](https://spraakbanken.gu.se/resurser/lexicon).

This API makes it possible to search in the resources, read the entries, and also to get statistical information. For example:
- what are the different senses of word \\<X\\> in \\<lexicon\\>?
- how many (and which) of the resources has an entry with baseform "bord"?
- what is the frequency distribution of part-of-speech tags in \\<lexicon\\>?
"""

app = FastAPI(
    title="Karp-S API",
    summary="Exposes Språkbanken's lexical resources.",
    description=api_description,
    version="1.0-dev",
    docs_url=None,
    redoc_url="/",
)

config: Config = get_config()


compile_param_description = """
A list of fields to compile statistics on, for example `baseform`, `pos`, `normalized_form`
"""
resources_param_description = """
A comma-separated list of resource ID:s, for example `saldo`
"""
columns_param_description = """
Used for comma-separated values of and =-separated parameters like `field1=val_field1,field2=val_field2`
- left-hand side of `=` the values in the field will create one column each
- right-hand side of `=` is the value to be shown in the cell, for example part-of-speech.

It is mostly useful for smaller value sets. Selecting all entries, compiling on part-of-speech and showing baseform will create a weird table, the opposite will not.

The total and values in compile will always be shown and does not need to be added here.
"""


def get_list_param(**kwargs):
    """
    Used for comma-separated query parameters
    """

    def inner(list_str: str | None = Query(**kwargs, min_length=1, pattern="^[^,]+(,[^,]+)*$")) -> list[str]:
        return list_str.split(",")

    return inner


def get_columns_param(name: str):
    # TODO if = is omitted, can we simply show the available values and use field name as heading
    # TODO RHS could be a field and an operation, for example average. like count(*) in SQL or avg(freq) where freq is a field
    def inner(
        columns: str | None = Query(
            None, alias="columns", title="Extra columns", description=columns_param_description
        ),
    ) -> list[tuple[str, str]]:
        if not columns:
            return columns
        return [column_setting.split("=") for column_setting in columns.split(",")]

    return inner


def get_q_param():
    return Query(
        None,
        title="Query",
        description="The query. See http://ws.spraakbanken.gu.se/ws/karp/v7 for a description of the query language.",
    )


def get_resources_param():
    return get_list_param(alias="resources", title="Resources", description=resources_param_description)


@app.get("/config", summary="Get config")
def get_config() -> list[ResourceConfig]:
    """
    Returns a description of contents of each installed resource/lexicon. For example the available fields and their types.
    """
    return list(get_resource_configs())


@app.get("/search", summary="Search")
def do_search(
    resources: list[str] = Depends(get_resources_param()),
    q: str | None = get_q_param(),
) -> SearchResult:
    """
    From each provided resource, return the entries that match the query q.
    """
    return search(config, resources, q=q)


@app.get("/count", summary="Count")
def do_count(
    resources: list[str] = Depends(get_resources_param()),
    q: str | None = get_q_param(),
    compile: list[str] = Depends(
        get_list_param(alias="compile", title="Compile on", description=compile_param_description)
    ),
    columns: list[list[str]] = Depends(get_columns_param("columns")),
) -> CountResult:
    """
    From each provided resource, get the entries that match the query q. See http://ws.spraakbanken.gu.se/ws/karp/v7 for a description of the query language.

    Compiled the matching entries on the fields in compile.

    Each column given in columns will be added to the result.
    """
    table = count(config, resources, q=q, compile=compile, columns=columns)
    return CountResult(table=table)
