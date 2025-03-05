from typing import Any
import pydantic
from pydantic import ConfigDict


def to_lower_camel(s: str) -> str:
    """Transform snake_case to lowerCamelCase."""

    return "".join(word.capitalize() if i > 0 else word for i, word in enumerate(s.split("_")))


class BaseModel(pydantic.BaseModel):
    """Base class for schema classes."""

    model_config = {"alias_generator": to_lower_camel, "populate_by_name": True}


class HitResponse(BaseModel):
    # entry can be anything
    entry: dict


class LexiconResult(BaseModel):
    hits: list[HitResponse]
    total: int


class SearchResult(BaseModel):
    hits: dict[str, LexiconResult]
    total: int


class CountResult(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    table: list[dict[str, Any]]
