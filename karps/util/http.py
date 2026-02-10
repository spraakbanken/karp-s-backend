from dataclasses import dataclass
import json

from typing import Any, Mapping
import urllib.error
import urllib.request


@dataclass
class Response:
    status: int
    body_text: str | None = None

    def json(self) -> Mapping[str, Any]:
        return json.loads(self.body_text) if self.body_text else {}


def post(url: str, headers=None, json_data=None) -> Response:
    """
    Very basic way to post a request and get an answer.
    response: Response = post(...)
    data = response.json()

    Tries to catch HTTPErrors and always return status and body.
    """
    if not headers:
        headers = {}
    if json_data:
        payload = json.dumps(json_data).encode("utf-8")
        headers["Content-Type"] = "application/json"
    else:
        payload = b""

    req = urllib.request.Request(
        url=url,
        data=payload,
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(req) as resp:
            status = resp.getcode()
            body_bytes = resp.read()
            body_text = body_bytes.decode("utf-8")
    except urllib.error.HTTPError as e:
        status = e.getcode()
        if not status:
            raise e
        body_text = e.read().decode("utf-8")
    return Response(status, body_text)
