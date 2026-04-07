from karps.config import Field, MainConfig
from karps.query.query import SubQuery, parse_query, get_query


dummy_config = MainConfig(
    tags={},
    fields={
        "field": Field(name="field", type="str"),
        "field1": Field(name="field1", type="str"),
        "field2": Field(name="field2", type="str"),
        "field3": Field(name="field3", type="str", collection=True),
    },
)


def test_parse():
    q = parse_query("equals|field|value")
    assert isinstance(q, SubQuery)
    assert q.op == "equals"
    assert q.field == "field"
    assert q.value == "value"


def test_sql_query():
    ast = parse_query("equals|field|value")
    fields, (query, params), collection_queries = get_query(dummy_config, "", ast)
    assert fields == {"field"}
    assert query == "`field` = %s"
    assert params == ("value",)
    assert collection_queries == []


def test_and_1():
    ast = parse_query("and(equals|field|value1)")
    fields, (query, params), collection_queries = get_query(dummy_config, "", ast)
    assert query == "`field` = %s"
    assert params == ("value1",)
    assert fields == {"field"}
    assert collection_queries == []


def test_or_1():
    ast = parse_query("or(equals|field|value1)")
    fields, (query, params), collection_queries = get_query(dummy_config, "", ast)
    assert query == "`field` = %s"
    assert params == ("value1",)
    assert fields == {"field"}
    assert collection_queries == []


def test_and_2():
    ast = parse_query("and(equals|field1|value1||equals|field2|value2)")
    fields, (query, params), collection_queries = get_query(dummy_config, "", ast)
    assert fields == {"field1", "field2"}
    assert query == "`field1` = %s AND `field2` = %s"
    assert params == ("value1", "value2")
    assert collection_queries == []


def test_not():
    ast = parse_query("not(equals|field|value)")
    _, (query, params), _ = get_query(dummy_config, "", ast)
    assert query == "NOT `field` = %s"
    assert params == ("value",)


def test_complex_query():
    q_in = "equals|field|value"
    q_out = "`field` = %s"
    ast = parse_query(f"and(or(not({q_in})||{q_in})||{q_in})")
    _, (query, params), _ = get_query(dummy_config, "", ast)
    assert query == f"((NOT {q_out}) OR {q_out}) AND {q_out}"
    assert params == ("value", "value", "value")


def test_collection_field():
    """
    Test that query will get the correct WHERE clause when searching in
    field3 - with collection: true in non-boolean query
    """
    ast = parse_query("equals|field3|value")
    fields, (query, params), collection_queries = get_query(dummy_config, "", ast)
    assert query == "EXISTS (SELECT 1 FROM `field3_0__where` WHERE TABLE_PREFIX__id = __parent_id)"
    assert params == ()
    inner_q_out = "`field3` = %s"
    assert collection_queries[0] == ("field3", 0, (inner_q_out, ("value",)))
    assert len(collection_queries) == 1
    assert fields == {"field3"}


def test_collection_field_multi_clause():
    """
    Test that query will get the correct WHERE clause when searching in
    field3 - with collection: true - twice in a logical query.
    """
    ast = parse_query("or(equals|field3|value0||equals|field3|value1)")
    fields, (query, params), collection_queries = get_query(dummy_config, "", ast)
    q_out = "EXISTS (SELECT 1 FROM `field3_{idx}__where` WHERE TABLE_PREFIX__id = __parent_id)"
    assert query == f"{q_out.format(idx=0)} OR {q_out.format(idx=1)}"
    assert params == ()
    assert len(collection_queries) == 2
    for idx in range(2):
        assert collection_queries[idx] == ("field3", idx, ("`field3` = %s", (f"value{idx}",)))
    assert fields == {"field3"}
