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
    resource_id: str


class SearchResult(BaseModel):
    hits: list[HitResponse]
    resource_hits: dict[str, int]
    resource_order: list[str]
    total: int


class UserErrorResult(BaseModel):
    message: str


type Scalar = str | int | float | bool


class CountResult(BaseModel):
    headers: list[dict[str, str]]
    table: list[list[Scalar | list[Scalar] | None]]


class UserErrorSchema(BaseModel):
    message: str
