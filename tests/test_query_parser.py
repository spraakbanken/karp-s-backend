from karps.config import Field, MainConfig
from karps.query.query import parse_query, get_query


def test_parse():
    q = parse_query("equals|field|value")
    assert q
    assert q.op == "equals"
    assert q.field == "field"
    assert q.value == "value"


def test_sql_query():
    ast = parse_query("equals|field|value")
    dummy_config = MainConfig(tags={}, fields={"field": Field(name="field", type="str")})
    field, op_arg = get_query(dummy_config, "", ast)
    assert field == "field"
    assert op_arg == "`field` = 'value'"
