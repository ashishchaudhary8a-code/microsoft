from streaming.app.normalize import normalize_event


def test_unknown_fields_ignored_and_utc_normalized():
    event = normalize_event(
        {
            "event_id": "EVT_1",
            "organization_id": "ORG_X",
            "domain": "warehouse",
            "event_type": "event",
            "timestamp": "2026-09-11T08:01:00+05:30",
            "extra_hospital_only_field": "nope",
            "payload": None,
            "metadata": None,
            "amount": 10,
        }
    )
    assert event["timestamp"].endswith("Z")
    assert "extra_hospital_only_field" not in event
    assert event["payload"] == {}
    assert event["metadata"] == {}
    assert event["amount"] == 10.0
