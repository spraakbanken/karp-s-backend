def test_v2_spec(snapshot, monkeypatch):
    monkeypatch.setenv("API_VERSION", "v2")

    from karps.api import app

    assert app.openapi() == snapshot
