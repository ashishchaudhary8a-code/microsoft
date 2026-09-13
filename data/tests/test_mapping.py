from pathlib import Path

from data.historical_load import load_mapping, map_row


def test_retail_mapping_is_generic():
    mapping = load_mapping(Path("data/mappings/retail.json"))
    row = {
        "order_id": "ORD_1",
        "order_ts": "2026-09-01T12:00:00Z",
        "associate_id": "USR_R01",
        "store_city": "Boston",
        "order_total": "42.5",
    }
    event = map_row(row, mapping, 0)
    assert event is not None
    assert event["organization_id"] == "ORG_RETAIL_001"
    assert event["domain"] == "retail"
    assert event["event_id"] == "ORD_1"
    assert event["amount"] == 42.5
    assert "hospital" not in event["domain"]


def test_bad_row_skipped():
    mapping = load_mapping(Path("data/mappings/hospital.json"))
    assert map_row({"txn_id": "x"}, mapping, 1) is None
