from streaming.app.validate import ValidationError, validate_event


def valid_event(**overrides):
    base = {
        "event_id": "EVT_1",
        "organization_id": "ORG_RETAIL_001",
        "domain": "retail",
        "event_type": "event",
        "timestamp": "2026-09-11T02:31:00Z",
        "amount": 12.5,
    }
    base.update(overrides)
    return base


def test_accepts_canonical_event():
    out = validate_event(valid_event())
    assert out["event_id"] == "EVT_1"


def test_missing_required_rejected():
    try:
        validate_event(valid_event(timestamp=""))
        assert False, "expected ValidationError"
    except ValidationError as exc:
        assert exc.code == "VALIDATION_ERROR"


def test_invalid_timestamp():
    try:
        validate_event(valid_event(timestamp="not-a-date"))
        assert False
    except ValidationError:
        pass


def test_amount_must_be_number():
    try:
        validate_event(valid_event(amount="lots"))
        assert False
    except ValidationError as exc:
        assert "amount" in exc.message
