from karps.query.query import parse_query, get_query


def test_parse():
    q = parse_query("equals|field|value")
    assert q
    assert q.op == "equals"
    assert q.field == "field"
    assert q.value == "value"


def test_sql_query():
    ast = parse_query("equals|field|value")
    field, op_arg = get_query("", ast)
    assert field == "field"
    assert op_arg == "= 'value'"
