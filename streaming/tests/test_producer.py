from streaming.app.metrics import percentile, record_processing_latency, processing_latency_p95_seconds
from streaming.app.producer import dlq_envelope, message_key, send_with_retry


class FlakyProducer:
    def __init__(self, fail_times: int):
        self.fail_times = fail_times
        self.calls = 0
        self.sent = []

    def send(self, topic, key=None, value=None):
        self.calls += 1
        if self.calls <= self.fail_times:
            raise RuntimeError("broker blip")
        self.sent.append((topic, key, value))

        class Ok:
            def get(self, timeout=None):
                return None

        return Ok()


def test_send_retries_then_succeeds(monkeypatch):
    monkeypatch.setattr("streaming.app.producer.time.sleep", lambda *_: None)
    prod = FlakyProducer(fail_times=2)
    send_with_retry(prod, "events.processed", b"ORG:U", {"ok": True}, attempts=3)
    assert len(prod.sent) == 1


def test_send_retries_then_raises(monkeypatch):
    monkeypatch.setattr("streaming.app.producer.time.sleep", lambda *_: None)
    prod = FlakyProducer(fail_times=9)
    try:
        send_with_retry(prod, "events.processed", b"ORG:U", {"ok": True}, attempts=3)
        assert False, "expected RuntimeError"
    except RuntimeError as exc:
        assert "3 attempts" in str(exc)


def test_dlq_envelope_shape():
    env = dlq_envelope("events.raw", "VALIDATION_ERROR", "missing timestamp", {"event_id": "X"})
    assert env["original_topic"] == "events.raw"
    assert env["error_code"] == "VALIDATION_ERROR"
    assert "failed_at" in env
    assert env["payload"]["event_id"] == "X"


def test_message_key_without_user():
    assert message_key({"organization_id": "ORG_A", "user_id": None}) == b"ORG_A"


def test_p95_latency_gauge():
    record_processing_latency(0.01)
    record_processing_latency(0.02)
    record_processing_latency(1.0)
    assert processing_latency_p95_seconds._value.get() >= 0.02
    assert percentile([1.0, 2.0, 3.0, 4.0], 0.95) == 4.0
