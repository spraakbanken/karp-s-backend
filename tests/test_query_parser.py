from karps.config import Field, MainConfig
from karps.query.query import SubQuery, parse_query, get_query


dummy_config = MainConfig(
    tags={},
    fields={
        "field": Field(name="field", type="str"),
        "field1": Field(name="field1", type="str"),
        "field2": Field(name="field2", type="str"),
    },
)

# TODO add test for collection


def test_parse():
    q = parse_query("equals|field|value")
    assert isinstance(q, SubQuery)
    assert q.op == "equals"
    assert q.field == "field"
    assert q.value == "value"


def test_sql_query():
    ast = parse_query("equals|field|value")
    fields, query, collection_queries = get_query(dummy_config, "", ast)
    assert fields == ["field"]
    assert query == "`field` = 'value'"
    assert collection_queries == []


def test_and_1():
    ast = parse_query("and(equals|field|value1)")
    fields, query, collection_queries = get_query(dummy_config, "", ast)
    assert query == "`field` = 'value1'"
    assert fields == ["field"]
    assert collection_queries == []


def test_or_1():
    ast = parse_query("or(equals|field|value1)")
    fields, query, collection_queries = get_query(dummy_config, "", ast)
    assert query == "`field` = 'value1'"
    assert fields == ["field"]
    assert collection_queries == []


def test_and_2():
    ast = parse_query("and(equals|field1|value1||equals|field2|value2)")
    fields, query, collection_queries = get_query(dummy_config, "", ast)
    assert fields == ["field1", "field2"]
    assert query == "`field1` = 'value1' AND `field2` = 'value2'"
    assert collection_queries == []
