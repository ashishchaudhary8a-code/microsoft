from streaming.app.dedupe import EventDeduper


def test_dedupe_same_org_event():
    d = EventDeduper(ttl_seconds=60)
    assert d.seen("ORG_A", "EVT_1") is False
    assert d.seen("ORG_A", "EVT_1") is True
    assert d.seen("ORG_B", "EVT_1") is False


def test_dedupe_ttl_expires(monkeypatch):
    clock = {"now": 1000.0}

    monkeypatch.setattr("streaming.app.dedupe.time.time", lambda: clock["now"])
    d = EventDeduper(ttl_seconds=10)
    assert d.seen("ORG_A", "EVT_1") is False
    clock["now"] = 1011.0
    assert d.seen("ORG_A", "EVT_1") is False

