from contextlib import contextmanager
import multiprocessing
import sys
from time import sleep
from typing import Iterable
import pytest
import requests
import uvicorn

from karps.api import app
from karps.config import ConfigResponse
from karps.models import CountResult, SearchResult


class Backend:
    def __init__(self):
        self.port = 12345

    def get(self, path: str):
        url = f"http://localhost:{self.port}{path}"
        return requests.get(url)

    def config(self) -> ConfigResponse:
        response = self.get("/config")
        return ConfigResponse(**response.json())

    def search(self, resource_ids: list[str], q_str: str = "") -> SearchResult:
        response = self.get(f"/search?resources={','.join(resource_ids)}&{q_str.replace('+', '%2b')}")
        json_data = response.json()
        return SearchResult(**json_data)

    def count(
        self,
        resource_ids: list[str],
        q_str: str = "",
        compile: Iterable[str] = ("word",),
        columns: str = "resource_id=partOfSpeech",
    ) -> CountResult:
        response = self.get(
            f"/count?resources={','.join(resource_ids)}&{q_str}&compile={','.join(compile).replace('+', '%2b')}&columns={columns.replace('+', '%2b')}"
        )
        json_data = response.json()
        return CountResult(**json_data)

    def run_app(self):
        uvicorn.run(app, host="127.0.0.1", port=self.port)

    @contextmanager
    def start(self):
        process = multiprocessing.Process(target=self.run_app)
        try:
            process.start()
            # when debugging, the FastAPI process is slower to start
            if is_debugger_attached():
                sleep(1)
            else:
                sleep(0.1)
            yield
        finally:
            process.terminate()
            process.join()


def is_debugger_attached():
    try:
        import debugpy  # type: ignore

        return debugpy.is_client_connected()
    except:  # noqa: E722
        return sys.gettrace() is not None


@pytest.fixture
def backend():
    backend = Backend()
    with backend.start():
        yield backend
