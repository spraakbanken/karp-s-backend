from typing import Optional
from fastapi import FastAPI, Query

from karps.config import Config, ResourceConfig, get_config
from karps.search import search
from karps.models import SearchResult

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
