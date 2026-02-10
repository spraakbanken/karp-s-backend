from dataclasses import dataclass
from typing import Annotated, Any, Sequence

import pydantic

from karps.errors import errors


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


class Header(BaseModel):
    type: str  # can be value, total or compile
    column_field: str | None = None  # when type=value


class ValueHeader(Header):
    header_field: str  # when type=value or total
    header_value: str  # when header_field is available


class CountCellValue(BaseModel):
    count: int
    value: Scalar | list[Scalar] | None


class CountCell(BaseModel):
    count: int
    values: list[CountCellValue] = []


class CountResult(BaseModel):
    headers: list[Header | ValueHeader]
    # TODO can we make it clearer which value may appear where?
    # table/total[0:len(compile)] == scalar | list[Scalar], table/total[len(compile)] == int (count column), table/total[len(compile) + 1:] == CountCell
    table: Annotated[list[list[Scalar | list[Scalar] | list[list[Scalar]] | CountCell | None]], pydantic.FailFast()]
    total: list[CountCell | int | str]


class UserErrorSchema(BaseModel):
    message: str = pydantic.Field(..., title="")
    code: int | None = pydantic.Field(
        None,
        title="",
        description="### Code definitions\n\n"
        + "\n\n".join([f"{error_code.code}: {error_code.description}" for error_code in errors.error_codes.values()]),
    )
    extra: dict[str, Any] | None = pydantic.Field(
        None, title="", description="Some errors have data output in addition to the human readable message."
    )


@dataclass
class Request:
    pass


@dataclass
class CountRequest(Request):
    compile: Sequence[str]
    columns: tuple[str, str]
