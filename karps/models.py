import pydantic


def to_lower_camel(s: str) -> str:
    """Transform snake_case to lowerCamelCase."""

    return "".join(word.capitalize() if i > 0 else word for i, word in enumerate(s.split("_")))


class BaseModel(pydantic.BaseModel):
    """Base class for schema classes."""

    model_config = {"alias_generator": to_lower_camel, "populate_by_name": True}


class HitResponse(BaseModel):
    # entry can be anything
    entry: dict[str, object]


class LexiconResult(BaseModel):
    hits: list[HitResponse]
    total: int


class SearchResult(BaseModel):
    hits: dict[str, LexiconResult]
    total: int


type Scalar = str | int | bool


class CountResult(BaseModel):
    headers: list[str]
    table: list[list[Scalar | list[Scalar] | None]]


class UserErrorSchema(BaseModel):
    message: str
