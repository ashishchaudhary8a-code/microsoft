from streaming.app.dedupe import EventDeduper


def test_dedupe_same_org_event():
    d = EventDeduper(ttl_seconds=60)
    assert d.seen("ORG_A", "EVT_1") is False
    assert d.seen("ORG_A", "EVT_1") is True
    assert d.seen("ORG_B", "EVT_1") is False
