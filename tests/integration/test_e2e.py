from tests.integration.conftest import Backend


"""
These tests depend on ao, fulaord and kelly
"""


def test_search(backend: Backend, snapshot):
    entries = backend.search(["ao"], q="equals|entry_word|eventuell")
    assert entries == snapshot


def test_search_with_from(backend: Backend, snapshot):
    result = backend.search(["ao", "kelly"], q="equals|ud_pos|NOUN")
    assert result == snapshot


def test_search_three_resources(backend: Backend, snapshot):
    # should give one fulaord and 9 kelly
    result = backend.search(["kelly", "ao", "fulaord"], q="startswith|entry_word|a", from_=577)
    assert result == snapshot


def test_search_three_resources2(backend: Backend, snapshot):
    result = backend.search(["ao", "kelly", "fulaord"], q="startswith|entry_word|a")
    ao_count = result.resource_hits["ao"]
    kelly_count = result.resource_hits["kelly"]
    result = backend.search(["kelly", "ao", "fulaord"], q="startswith|entry_word|a", from_=ao_count + kelly_count)
    assert result == snapshot


def test_search_with_too_large_from(backend: Backend, snapshot):
    result = backend.search(["ao", "kelly"], q="equals|ud_pos|NOUN")
    ao_count = result.resource_hits["ao"]
    kelly_count = result.resource_hits["kelly"]

    result, status = backend.search_with_status(["ao", "kelly"], q="equals|ud_pos|NOUN", from_=ao_count + kelly_count)
    assert result, status == snapshot


def test_zero_hits(backend: Backend, snapshot):
    result, status = backend.search_with_status(["ao", "kelly"], q="equals|ud_pos|ASDFGHJKL")
    assert result, status == snapshot


def test_count(backend: Backend, snapshot):
    resource_id = "ao"
    entries = backend.count([resource_id], q="equals|entry_word|eventuell")
    assert entries == snapshot
