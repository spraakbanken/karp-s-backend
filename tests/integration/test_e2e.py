from tests.integration.conftest import Backend

# These tests depend on a resource called ao with word containing eventuell and a field called partOfSpeech


def test_search(backend: Backend):
    """
    This test depends on a resource called ao available with word as str and non-collection
    """
    resource_id = "ao"
    entries = backend.search([resource_id], "q=equals|word|eventuell")
    assert entries.total > 0
    assert entries.hits[resource_id].total > 0


def test_count(backend: Backend):
    resource_id = "ao"
    entries = backend.count([resource_id], "q=equals|word|eventuell")
    assert entries.headers == ["word", "ao", "total"]
    assert len(entries.table) > 0
