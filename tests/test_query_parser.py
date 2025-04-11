from karps.query.query import parse_query, as_sql


def test_parse():
    q = parse_query("equals|field|value")
    assert q.op == "equals"
    assert q.field == "field"
    assert q.value == "value"

def test_sql_query():
    ast = parse_query("equals|field|value")
    sql_str = as_sql(ast)
    assert sql_str == "WHERE `field` = 'value'"
