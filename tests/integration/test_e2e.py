from karps.models import SearchResult, UserErrorResult
from tests.integration.conftest import Backend


def test_search(backend: Backend):
    """
    This test depends on a resource called ao available with word as str and non-collection
    """
    resource_id = "ao"
    entries = backend.search([resource_id], "q=equals|word|eventuell")
    assert entries.total > 0
    assert entries.resource_hits[resource_id] > 0


def test_search_with_from(backend: Backend):
    result = backend.search(["ao", "kelly"], "q=equals|partOfSpeech|nn")
    # assert expected size
    assert len(result.hits) == 10
    # assert expected totals
    assert result.total > 10
    # assert the number of hits are the expected size
    assert len(result.hits) == 10
    # for each hit, check that resource_id is correct
    for hit in result.hits:
        assert hit.resource_id == "ao"

    # use resource_hits to find where the ao results start
    ao_count = result.resource_hits["ao"]
    # we expected 1 entry from ao and 9 from kelly
    result2 = backend.search(["ao", "kelly"], "q=equals|partOfSpeech|nn", from_=ao_count - 1)
    assert len(result2.hits) == 10
    assert result2.total > 10

    # first hit should be ao
    assert result.hits[0].resource_id == "ao"
    # the rest should be kelly
    for hit in result2.hits[1:]:
        assert hit.resource_id == "kelly"

    # resource_hits and total should be the same no matter what page we are fetching
    assert result.resource_hits == result2.resource_hits
    assert result.total == result2.total


def test_search_three_resources(backend: Backend):
    # should give one fulaord and 9 kelly
    result = backend.search(["kelly", "ao", "fulaord"], "q=startswith|word|a&from=577")
    assert len(result.hits) == 10
    assert result.resource_order == ["ao", "fulaord", "kelly"]
    assert result.hits[0].resource_id == "fulaord"
    for hit in result.hits[1:]:
        assert hit.resource_id == "kelly"


def test_search_three_resources2(backend: Backend):
    # should give one fulaord and 9 kelly
    result = backend.search(["kelly", "ao", "fulaord"], "q=startswith|word|a&from=578")
    assert len(result.hits) == 10
    assert result.resource_order == ["ao", "fulaord", "kelly"]
    for hit in result.hits[0:]:
        assert hit.resource_id == "kelly"


def test_search_with_too_large_from(backend: Backend):
    result = backend.search(["ao", "kelly"], "q=equals|partOfSpeech|nn")
    ao_count = result.resource_hits["ao"]
    kelly_count = result.resource_hits["kelly"]
    result, status = backend.search_with_status(
        ["ao", "kelly"], "q=equals|partOfSpeech|nn", from_=ao_count + kelly_count
    )
    assert isinstance(result, UserErrorResult)
    assert status == 500
    assert "from" in result.message


def test_zero_hits(backend: Backend):
    result, status = backend.search_with_status(["ao", "kelly"], "q=equals|partOfSpeech|ASDFGHJKL")
    assert isinstance(result, SearchResult)
    ao_count = result.resource_hits["ao"]
    kelly_count = result.resource_hits["kelly"]
    assert ao_count == kelly_count == 0
    assert status != 500
    assert result.hits == []


def test_count(backend: Backend):
    resource_id = "ao"
    entries = backend.count([resource_id], "q=equals|word|eventuell")
    assert entries.headers == ["word", "ao", "total"]
    assert len(entries.table) > 0
