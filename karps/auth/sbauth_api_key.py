from typing import Any, Mapping
from karps.errors.errors import ApiKeyError
from karps.util import http


def get_scope(api_key, url: str, sbauth_api_key: str) -> Mapping[str, Any]:
    headers = {
        "Authorization": f"apikey {sbauth_api_key}",
    }
    data = {"apikey": api_key}

    response = http.post(url, headers=headers, json_data=data)
    if response.status != 200:
        raise ApiKeyError()
    return response.json()["scope"]
