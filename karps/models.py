from pydantic import BaseModel, ConfigDict


class HitResponse(BaseModel):
    # any can be anything
    entry: dict


class LexiconResult(BaseModel):
    hits: list[HitResponse]
    total: int


class SearchResult(BaseModel):
    hits: dict[str, LexiconResult]
    total: int


class CountResult(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    table: list[dict[str, any]]
