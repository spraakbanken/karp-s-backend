from karps.config import Field, MainConfig
from karps.query.query import parse_query, get_query


dummy_config = MainConfig(
    tags={},
    fields={
        "field": Field(name="field", type="str"),
        "field1": Field(name="field1", type="str"),
        "field2": Field(name="field2", type="str"),
    },
)


def test_parse():
    q = parse_query("equals|field|value")
    assert q.clauses
    assert q.clauses[0].op == "equals"
    assert q.clauses[0].field == "field"
    assert q.clauses[0].value == "value"


def test_sql_query():
    ast = parse_query("equals|field|value")
    _, [(field, op_arg)] = get_query(dummy_config, "", ast)
    assert field == "field"
    assert op_arg == "`field` = 'value'"


def test_and_1():
    ast = parse_query("and(equals|field|value1)")
    op, elems = get_query(dummy_config, "", ast)
    assert op == "and"
    assert isinstance(elems, list)
    for field, op_arg in elems:
        assert field == "field"
        assert op_arg == "`field` = 'value1'"


def test_or_1():
    ast = parse_query("or(equals|field|value1)")
    op, elems = get_query(dummy_config, "", ast)
    assert op == "or"
    assert isinstance(elems, list)
    for field, op_arg in elems:
        assert field == "field"
        assert op_arg == "`field` = 'value1'"


def test_and_2():
    ast = parse_query("and(equals|field1|value1||equals|field2|value2)")
    op, elems = get_query(dummy_config, "", ast)
    assert op == "and"
    assert isinstance(elems, list)
    field1, op_arg1 = elems[0]
    assert field1 == "field1"
    assert op_arg1 == "`field1` = 'value1'"
    field2, op_arg2 = elems[1]
    assert field2 == "field2"
    assert op_arg2 == "`field2` = 'value2'"
