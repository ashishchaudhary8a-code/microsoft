from streaming.app.alerts import BurstTracker
from streaming.app.processor import StreamProcessor


class FakeProducer:
    def __init__(self):
        self.sent = []

    def send(self, topic, key=None, value=None):
        self.sent.append((topic, key, value))

    def flush(self):
        pass


def _event(i, score_hint_amount=4000):
    return {
        "event_id": f"EVT_{i}",
        "organization_id": "ORG_HOSPITAL_001",
        "domain": "hospital",
        "event_type": "event",
        "timestamp": f"2026-09-11T10:0{i}:00Z",
        "user_id": "USR_204",
        "username": "employee_204",
        "location": "Pune",
        "device_id": "DEVICE_12",
        "ip_address": "10.0.0.14",
        "action": "payment",
        "status": "completed",
        "amount": score_hint_amount,
        "currency": "INR",
        "payload": {},
        "metadata": {},
    }


def test_invalid_goes_to_dlq_not_processed():
    prod = FakeProducer()
    proc = StreamProcessor(
        prod,
        {"raw": "events.raw", "processed": "events.processed", "anomalies": "anomalies.detected", "alerts": "alerts.generated", "dlq": "events.dlq"},
        artifact_dir="ml/artifacts",
        org_config_dir="data/orgs",
    )
    proc.process_raw({"event_id": "X"})
    topics = [t for t, _, _ in prod.sent]
    assert "events.dlq" in topics
    assert "events.processed" not in topics


def test_extreme_amount_emits_anomaly_and_alert():
    prod = FakeProducer()
    proc = StreamProcessor(
        prod,
        {"raw": "events.raw", "processed": "events.processed", "anomalies": "anomalies.detected", "alerts": "alerts.generated", "dlq": "events.dlq"},
        artifact_dir="ml/artifacts",
        org_config_dir="data/orgs",
    )
    proc.process_raw(
        {
            **_event(1, 250000),
            "location": "Unknown-City",
            "device_id": "DEVICE_991",
            "ip_address": "192.168.1.20",
            "timestamp": "2026-09-11T02:31:00Z",
        }
    )
    topics = [t for t, _, _ in prod.sent]
    assert "events.processed" in topics
    anomalies = [v for t, _, v in prod.sent if t == "anomalies.detected"]
    alerts = [v for t, _, v in prod.sent if t == "alerts.generated"]
    assert anomalies
    assert anomalies[0]["anomaly_score"] >= 0.0
    assert 0 <= anomalies[0]["anomaly_score"] <= 1
    if anomalies[0]["anomaly_score"] >= 0.90:
        assert any(a["alert_type"] == "SINGLE_EXTREME_EVENT" for a in alerts)


def test_burst_emits_once():
    org = {
        "suspicious_score_threshold": 0.0,
        "suspicious_event_window_seconds": 300,
        "suspicious_event_count": 3,
    }
    tracker = BurstTracker()
    fired = 0
    for i in range(5):
        ev = _event(i)
        window = tracker.observe(ev, 0.7, org)
        if window:
            fired += 1
    assert fired == 1


def test_no_user_skips_burst():
    tracker = BurstTracker()
    ev = _event(1)
    ev.pop("user_id")
    assert tracker.observe(ev, 0.9, {"suspicious_score_threshold": 0.6, "suspicious_event_count": 1, "suspicious_event_window_seconds": 300}) is None


def test_duplicate_dropped():
    prod = FakeProducer()
    proc = StreamProcessor(
        prod,
        {"raw": "events.raw", "processed": "events.processed", "anomalies": "anomalies.detected", "alerts": "alerts.generated", "dlq": "events.dlq"},
        artifact_dir="ml/artifacts",
        org_config_dir="data/orgs",
    )
    proc.process_raw(_event(1, 1000))
    before = len(prod.sent)
    proc.process_raw(_event(1, 1000))
    after = len(prod.sent)
    assert after == before
