from pydantic import BaseModel


class HitResponse(BaseModel):
    # any can be anything
    entry: dict


class LexiconResult(BaseModel):
    hits: list[HitResponse]
    total: int


class SearchResult(BaseModel):
    hits: dict[str, LexiconResult]
    total: int
