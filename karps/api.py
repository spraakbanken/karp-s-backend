import json
from typing import Any, Sequence
from fastapi import FastAPI, Depends, Query, Request, Response
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from karps.config import (
    Env,
    ConfigResponse,
    ResourceConfig,
    get_env,
    get_resource_config,
    get_resource_configs,
    load_config,
)
from karps.logging import setup_sql_logger
from karps.search import count, search
from karps.models import SearchResult, UserErrorSchema
from karps.errors import errors
from karps.auth.deps import get_allowed_resources

api_description = """
Språkbanken has many lexical resources, listed on our [webpage](https://spraakbanken.gu.se/resurser/lexicon).

This API makes it possible to search the resources, read the entries, and also to get statistical information. For example:
- What are the different senses of word \\<X\\> in \\<lexicon\\>?
- How many (and which) of the resources have an entry with baseform "bord"?
- What is the frequency distribution of part-of-speech tags in \\<lexicon\\>?

## The basics

A **resource** is a collection of **entries** with the same fields (or schema). Every entry
has a default field - `entryWord` - which is usually something like a lemma.

## Sorting the results

Ascending and descending sort are available. The sorting uses Swedish (POSIX `sv_SE` or MariaDB `utf8mb4_swedish_ci`) collation for text fields. It is also possible to 
sort by fields of other types. Sorting is done using the `sort` parameter with the following grammar:
```ebnf
sort          ::= order | multi_fields ;
order         ::= "asc" | "desc" ;
multi_fields  ::= field_sort ( "," field_sort )* ;
field_sort    ::= field_name "|" order ;
```
`field_name` is not defined in the grammar, see each API-call for more information about available fields.

Examples: `sort=desc`, `sort=entryWord,pos|desc,nativePos|desc`

The default value is `asc`. Only selecting an order uses the default field(s). See the respective search commands for defaults.

When sorting by multiple fields, the sort will be applied in the given order. `asc` is always used when order is emitted.
"""


app = FastAPI(
    title="Karp-s API",
    summary="Exposes Språkbanken's lexical resources.",
    description=api_description,
    version="1.0-dev",
    docs_url=None,
    redoc_url="/",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(errors.UserError)
async def exception_handler(request: Request, exc: errors.UserError):
    return JSONResponse(
        status_code=500,
        content={"message": str(exc)},
    )


@app.exception_handler(errors.CodeUserError)
async def exception_handler2(request: Request, exc: errors.CodeUserError):
    content = {"message": exc.msg, "code": exc.code}
    if exc.details:
        content["details"] = exc.details
    return JSONResponse(
        status_code=500,
        content=content,
    )


env: Env = get_env()
if env.sql_query_logging:
    setup_sql_logger(env.logging_dir)


compile_param_description = """
A list of fields to compile statistics on, for example `baseform`, `pos`, `normalized_form`
"""
resources_param_description = """
A comma-separated list of resource ID:s, for example `saldo`
"""
columns_param_description = """
Add extra data columns to the result. For example: `field1=val_field1,field2=val_field2`

The left-hand side of `=` denotes  a field that will be used to create columns. Good examples
are `resourceId` and `ud_pos`. For each unique value in the result, a column will be created 
and the field in the right-hand side of `=` 
will be used for values in the cells of that column. If there are multiple values for the right-hand side field they
will be shown as a list.

It is possible to replace the field of the right-hand side with the keyword `_count` - `columns=resourceId=_count`.
This is the default value if `columns` is omitted. With this, the number of
hits per unique value of the selected field will be shown.

The total number of rows and columns from `compile` will always be shown regardless of the value of `columns`.
"""

sort_param_description = """
See [Sorting the results](#section/Sorting-the-results) and API call description for more information.
"""


def normalize(elem):
    return elem.replace("entryWord", "entry_word").replace("resourceId", "resource_id")


def get_sort_param():
    """
    Used for the sort parameter, if "asc" or "desc", add field "_default
    """

    def inner(
        sort: str = Query("asc", description=sort_param_description),
    ) -> list[tuple[str, str]]:
        if sort in ["asc", "desc"]:
            # if only asc/desc and given, it might modifiy the default sort order
            return [("_default", sort)]
        sorts = []
        for elem in sort.split(","):
            parts = elem.split("|")
            if len(parts) == 2:
                field, sort_order = parts
                if sort_order in ["asc", "desc"]:
                    sorts.append((field, sort_order))
                else:
                    raise errors.UserError(f"Unsupported sort order: {sort_order}")
            else:
                # default sort order for given fields is asc
                if not parts[0]:
                    field = "_default"
                else:
                    field = parts[0]
                sorts.append((field, "asc"))
        return sorts

    return inner


def get_list_param(alias: str, title: str, description: str):
    """
    Used for comma-separated query parameters
    """

    def inner(
        list_str: str | None = Query(
            alias=alias, title=title, description=description, min_length=1, pattern="^[^,]+(,[^,]+)*$"
        ),
    ) -> list[str]:
        return normalize(list_str).split(",") if list_str else []

    return inner


def get_columns_param(name: str):
    def twotuple(elems: Sequence[str]) -> tuple[str, str]:
        if len(elems) != 2:
            raise errors.UserError("Columns parameter is wrongly formatted")
        return elems[0], elems[1]

    # TODO if = is omitted, can we simply show the available values and use field name as heading
    # TODO RHS could be a field and an operation, for example average. like count(*) in SQL or avg(freq) where freq is a field
    def inner(
        columns: str = Query(
            "resourceId=_count", alias="columns", title="Extra columns", description=columns_param_description
        ),
    ) -> list[tuple[str, str]]:
        if not columns:
            return [("resource_id", "_count")]
        return [twotuple(column_setting.split("=")) for column_setting in normalize(columns).split(",")]

    return inner


def get_q_param():
    return Query(
        None,
        title="Query",
        description='The query. See http://ws.spraakbanken.gu.se/ws/karp/v7 for a description of the query language, however, Karp-s does not support nesting boolean queries, the "not"-boolean query, sub-queries, exists, missing and freetext.',
    )


default_500: dict[int | str, dict[str, Any]] = {500: {"description": "Application error", "model": UserErrorSchema}}


def get_resource_configs_param():
    def inner(
        allowed_resources: list[str] = Depends(get_allowed_resources),
        resources: list[str] = Depends(
            get_list_param(alias="resources", title="Resources", description=resources_param_description)
        ),
    ) -> list[ResourceConfig]:
        resource_configs = []
        for resource in resources:
            resource_config = get_resource_config(env, resource)
            if resource_config.limited_access and resource_config.resource_id not in allowed_resources:
                raise errors.UserAccessError(resource_config.resource_id)
            resource_configs.append(resource_config)
        return resource_configs

    return inner


@app.get("/config", summary="Get config", response_model_exclude_unset=True)
def get_config() -> ConfigResponse:
    """
    Returns a description of contents of each installed resource/lexicon. For example the available fields and their types.
    """
    config = load_config(env)
    return ConfigResponse(tags=config.tags, fields=config.fields, resources=list(get_resource_configs(env)))


@app.get("/search", summary="Search", responses=default_500)
def do_search(
    resource_configs: list[ResourceConfig] = Depends(get_resource_configs_param()),
    q: str | None = get_q_param(),
    size: int = 10,
    _from: int = Query(0, alias="from"),
    sort: list[tuple[str, str]] = Depends(get_sort_param()),
) -> SearchResult:
    """
    From each provided resource, return the entries that match the query q.

    ### Sorting
    Sorting is supported on fields that are present in all selected resources. The default field is `entryWord` (**ascending** order).

    The sort is done within each resource, the results from each resource are not mixed.
    """
    main_config = load_config(env)
    return search(env, main_config, resource_configs, q=q, size=size, _from=_from, sort=sort)


@app.get("/count", summary="Count", response_model_exclude_none=True, responses=default_500)
def do_count(
    resource_configs: list[ResourceConfig] = Depends(get_resource_configs_param()),
    q: str | None = get_q_param(),
    compile: list[str] = Depends(
        get_list_param(alias="compile", title="Compile on", description=compile_param_description)
    ),
    columns: list[tuple[str, str]] = Depends(get_columns_param("columns")),
    sort: list[tuple[str, str]] = Depends(get_sort_param()),
) -> Response:
    """
    From each provided resource, get the entries that match the query q. See http://ws.spraakbanken.gu.se/ws/karp/v7 for a description of the query language.

    Compile the matching entries on the fields in compile.

    Add additional data requested by using the `columns` parameter.

    ### Sorting

    Sorting is supported for fields that are used in `compile`. The default fields are all the fields in `compile`
    (**ascending** order) (they themselves sorted alphabetically, just like the columns).
    """
    main_config = load_config(env)
    headers, table, total = count(env, main_config, resource_configs, q=q, compile=compile, columns=columns, sort=sort)
    headers_dumped = [header.model_dump(by_alias=True) for header in headers]
    # TODO fix response model for API-reference reasons
    result_str = json.dumps({"headers": headers_dumped, "table": table, "total": total}, ensure_ascii=False)
    return Response(result_str, media_type="application/json")
