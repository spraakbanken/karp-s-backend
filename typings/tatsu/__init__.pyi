class InnerQueryTree:
    arg: list[str] | str | int | bool
    op: str
    field: str

class QueryTree:
    op: str
    args: list[InnerQueryTree]

class Parser:
    def parse(
        self,
        text: str,
    ) -> QueryTree: ...

def compile(
    grammar: str,
) -> Parser: ...
