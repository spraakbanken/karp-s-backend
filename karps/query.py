from dataclasses import dataclass
from typing import Any


@dataclass
class Query:
    """
    Uses the same query language as Karp, described here:
    https://spraakbanken4.it.gu.se/karp/v7/#tag/Searching
    Currently, support in this class in very limited
    """

    op: str = None
    field: str = None
    value: Any = None


def parse_query(q: str):
    if not q:
        return None
    return Query(**dict(zip(["op", "field", "value"], q.split("|"))))
