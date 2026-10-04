from data.generators.generate import burst_events, extreme_event, load_org, normal_event
from data.historical_load import load_mapping, map_row
from pathlib import Path
import random


def test_org_configs_exist_and_are_generic():
    for org_id in ("ORG_HOSPITAL_001", "ORG_HOTEL_001", "ORG_RETAIL_001"):
        org = load_org(org_id)
        assert org["organization_id"] == org_id
        assert "domain" in org
        assert "anomaly_score_threshold" in org


def test_scenarios_emit_canonical_events():
    org = load_org("ORG_HOTEL_001")
    rng = random.Random(0)
    normal = normal_event(org, rng)
    extreme = extreme_event(org, rng)
    burst = burst_events(org, rng, 4)
    for ev in [normal, extreme, *burst]:
        for field in ("event_id", "organization_id", "domain", "event_type", "timestamp"):
            assert ev.get(field)
        assert ev["organization_id"] == "ORG_HOTEL_001"
        assert "transaction" not in ev["event_type"] or ev["event_type"] == "event"


def test_hotel_mapping():
    mapping = load_mapping(Path("data/mappings/hotel.json"))
    row = {
        "booking_id": "B1",
        "created_at": "2026-09-01T12:00:00Z",
        "clerk_id": "USR_H01",
        "property_city": "Goa",
        "total": "1200",
    }
    event = map_row(row, mapping, 0)
    assert event is not None
    assert event["domain"] == "hotel"
    assert event["event_id"] == "B1"
    assert event["amount"] == 1200.0
