class QueryTree:
    arg: list[str] | str | int | bool
    op: str | None
    field: str | None

class Parser:
    def parse(
        self,
        text: str,
    ) -> QueryTree: ...

def compile(
    grammar: str,
) -> Parser: ...
