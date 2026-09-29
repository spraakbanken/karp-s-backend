def test_v1_spec(snapshot, monkeypatch):
    monkeypatch.setenv("API_VERSION", "v1")

    from karps.api import app

    assert app.openapi() == snapshot
