from streaming.app.features import FeatureState, build_features
from streaming.app.producer import message_key


def test_message_key_prefers_user():
    assert message_key({"organization_id": "ORG_A", "user_id": "U1"}) == b"ORG_A:U1"
    assert message_key({"organization_id": "ORG_A"}) == b"ORG_A"


def test_feature_prep_without_baselines():
    state = FeatureState()
    event = {
        "event_id": "EVT_1",
        "organization_id": "ORG_X",
        "domain": "warehouse",
        "event_type": "event",
        "timestamp": "2026-09-11T10:00:00Z",
        "user_id": "U1",
        "amount": 10,
        "location": "Delhi",
        "device_id": "D1",
        "ip_address": "1.1.1.1",
    }
    org = {"timezone": "UTC"}
    f1 = build_features(event, org, None, state)
    assert f1["amount"] == 10
    assert f1["events_last_5min"] == 1
    assert f1["hour_of_day"] == 10
    event2 = dict(event)
    event2["event_id"] = "EVT_2"
    event2["timestamp"] = "2026-09-11T10:01:00Z"
    f2 = build_features(event2, org, None, state)
    assert f2["events_last_5min"] == 2
    assert f2["time_since_previous_event_seconds"] == 60
