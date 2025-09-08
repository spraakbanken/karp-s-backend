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
from karps.models import CountResult, SearchResult, UserErrorResult


class Backend:
    def __init__(self):
        self.port = 12345

    def get(self, path: str):
        url = f"http://localhost:{self.port}{path}"
        return requests.get(url)

    def config(self) -> ConfigResponse:
        response = self.get("/config")
        return ConfigResponse(**response.json())

    def search(self, resource_ids: list[str], q: str | None = None, from_: int = 0) -> SearchResult:
        res, _ = self.search_with_status(resource_ids, q, from_)
        if isinstance(res, SearchResult):
            return res
        raise RuntimeError("Unexpected backend error")

    def search_with_status(
        self, resource_ids: list[str], q: str | None = None, from_: int = 0
    ) -> tuple[SearchResult | UserErrorResult, int]:
        q_str = f"&q={q}" if q else ""
        url = f"/search?resources={','.join(resource_ids)}&{q_str.replace('+', '%2b')}"
        if from_ > 0:
            url += f"&from={from_}"
        response = self.get(url)
        json_data = response.json()
        if response.status_code == 500:
            return UserErrorResult(**json_data), 500
        else:
            return SearchResult(**json_data), response.status_code

    def count(
        self,
        resource_ids: list[str],
        q: str | None = None,
        compile: Iterable[str] = ("entry_word",),
        columns: str = "resource_id=ud_pos",
    ) -> CountResult:
        q_str = f"&q={q}" if q else ""
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
