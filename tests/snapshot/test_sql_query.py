from karps.config import Field, MainConfig, MultiLang, ResourceConfig
from karps.database.database import add_aggregation, get_search
from karps.query.query import parse_query

# NOTE in this code, snapshot is a fixture from the Syrupy snapshot testing library

# TODO
# 1. Test doesn't currenlty capture errors due to paging
# 2. Add fixture db that creates mock tables (and removes them in teardown)
#    use db to test that each snapshot is executable, no data needed in tables

main_config = MainConfig(
    tags={},
    fields={
        "data1": Field(name="data1", type="string"),
        "data2": Field(name="data2", type="string"),
        "data3": Field(name="data3", type="string"),
        "col1": Field(name="col1", type="string", collection=True),
        "col2": Field(name="col2", type="string", collection=True),
    },
)


def create_resource_config(idx):
    """Create a ResourceConfig for testing where idx is both in name and "word" field"""
    data_field = f"data{idx + 1}"
    return ResourceConfig(
        resource_id=f"r{idx}",
        label=MultiLang("r"),
        fields=[data_field, "data3", "col1", "col2"],
        word=data_field,
        word_description=MultiLang(data_field),
        updated=0,
        size=0,
        link="",
    )


resource_configs = [create_resource_config(idx) for idx in range(0, 2)]

# test configuration possibilities, querys, compiles and columns can be any of the three
SCALAR = "SCALAR"
COLLECTION = "COLLECTION"
WORD = "WORD"


def create_search_query(query=None):
    return create_search_queries(query=query, resource_configs=resource_configs[0:1])[0].to_string()


def create_search_queries(query=None, resource_configs=(), selection=None):
    q = None
    if query == COLLECTION:
        q = parse_query("equals|col1|1")
    elif query == SCALAR:
        q = parse_query("equals|data3|1")
    elif query == WORD:
        q = parse_query("equals|word|1")
    if selection:
        # count uses this
        return get_search(main_config, resource_configs, q, selection=selection)
    else:
        # search uses this (currently search does not allow selecting columns)
        return get_search(main_config, resource_configs, q)


def create_count_query(compile_type=None, columns_type=None, query=None):
    if compile_type is WORD:
        compile = ["word"]
    elif compile_type == SCALAR:
        compile = ["data3"]
    elif compile_type == COLLECTION:
        compile = ["col1"]
    else:
        raise RuntimeError("missing compile type given")
    if columns_type is None:
        columns = []
    elif columns_type == SCALAR:
        columns = [("resource_id", "data3")]
    elif columns_type == COLLECTION:
        columns = [("resource_id", "col1")]
    elif columns_type == WORD:
        columns = [("resource_id", "word")]
    else:
        raise RuntimeError("missing comulmns type given")

    # TODO these to rows are copied from search.py to add whatever was in compile and columns to the selection
    flattened_columns = [item for sublist in columns or () for item in sublist]
    selection = set(list(compile) + flattened_columns)

    queries = create_search_queries(query, resource_configs=resource_configs, selection=sorted(list(selection)))
    return add_aggregation(queries=queries, compile=compile, columns=flattened_columns).to_string()


def test_search1(snapshot):
    assert create_search_query() == snapshot(name="s1")


def test_search_2(snapshot):
    assert create_search_query(query=SCALAR) == snapshot(name="s2")


def test_search_3(snapshot):
    assert create_search_query(query=COLLECTION) == snapshot(name="s3")


# function for generating pytest functions, useful for reporting and being able to update single snapshots
def make_test_count(compile_type, columns_type, query_type):
    def test(snapshot):
        assert create_count_query(compile_type=compile_type, columns_type=columns_type, query=query_type) == snapshot(
            # using the configuration to name snapshot
            name=f"c{compile_type}_{columns_type}_{query_type}"
        )

    return test


# generate test-functions and add to the global namespace, so pytest can report multiple failures or syrupy update single snapshots
for idx_compile, compile_type in enumerate([WORD, SCALAR, COLLECTION]):
    for idx_columns, columns_type in enumerate([None, SCALAR, COLLECTION, WORD]):
        for idx_query, query_type in enumerate([None, SCALAR, COLLECTION, WORD]):
            # trying to name tests so we have a chance of figuring out which case went wrong
            globals()[f"test_count_compile-{compile_type}_columns-{columns_type}_query-{query_type}"] = make_test_count(
                compile_type, columns_type, query_type
            )
