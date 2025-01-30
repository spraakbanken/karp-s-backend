from typing import Optional
from fastapi import FastAPI, Query

from karps.config import Config, ResourceConfig, get_config
from karps.search import search, count
from karps.models import CountResult, SearchResult

app = FastAPI(docs_url=None, redoc_url="/")

config: Config = get_config()


@app.get("/config")
def config_api() -> ResourceConfig:
    return config.resource_config()


@app.get("/search")
def search_api(
    resources_str: str = Query(alias="resources", min_length=1),
    q: Optional[str] = Query(None),
) -> SearchResult:
    resources = resources_str.split(",")
    return search(config, resources, q=q)


@app.get("/count")
def count_api(
    resources_str: str = Query(alias="resources", min_length=1),
    q: str | None = Query(None),
    compile_str: str = Query(alias="compile"),
    columns_str: str | None = Query(None, alias="columns"),
) -> CountResult:
    resources = resources_str.split(",")
    compile = compile_str.split(",")
    columns = [column_setting.split("=") for column_setting in columns_str.split(",")] if columns_str else ()

    table = count(config, resources, q=q, compile=compile, columns=columns)
    return CountResult(table=table)
