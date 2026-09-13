"""Core stream processing: validate → normalize → dedupe → features → ML → produce."""

from __future__ import annotations

import json
import logging
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from ml.artifacts_store import ArtifactStore
from ml.scoring import Baselines, score_event
from streaming.app.alerts import BurstTracker, make_burst_alert, make_extreme_alert
from streaming.app.config import CONSUMER_ID, DEDUPE_TTL_SECONDS, ML_ARTIFACT_DIR, ORG_CONFIG_DIR
from streaming.app.dedupe import EventDeduper
from streaming.app.features import FeatureState, build_features
from streaming.app.metrics import (
    alerts_total,
    anomalies_total,
    critical_anomalies_total,
    events_deduped_total,
    events_dropped_total,
    events_failed_total,
    events_per_second,
    events_processed_total,
    events_received_total,
    ml_failures_total,
    record_processing_latency,
    service_health,
)
from streaming.app.normalize import normalize_event
from streaming.app.producer import dlq_envelope, message_key, send_with_retry
from streaming.app.validate import ValidationError, validate_event

log = logging.getLogger("streaming")

DEFAULT_ORG_CONFIG = {
    "baseline_lookback_days": 30,
    "anomaly_score_threshold": 0.75,
    "suspicious_score_threshold": 0.60,
    "suspicious_event_window_seconds": 300,
    "suspicious_event_count": 3,
    "freshness_sla_seconds": 5,
    "severity_bands": {
        "NORMAL": {"min": 0.00, "max": 0.39},
        "LOW": {"min": 0.40, "max": 0.59},
        "MEDIUM": {"min": 0.60, "max": 0.74},
        "HIGH": {"min": 0.75, "max": 0.89},
        "CRITICAL": {"min": 0.90, "max": 1.00},
    },
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_ms(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


class OrgConfigStore:
    def __init__(self, directory: str):
        self.directory = Path(directory)
        self._cache: dict[str, dict[str, Any]] = {}
        self.reload()

    def reload(self) -> None:
        self._cache.clear()
        if not self.directory.exists():
            return
        for path in list(self.directory.glob("*.json")):
            data = json.loads(path.read_text())
            org_id = data.get("organization_id") or path.stem
            merged = {**DEFAULT_ORG_CONFIG, **data}
            if "severity_bands" not in data:
                merged["severity_bands"] = DEFAULT_ORG_CONFIG["severity_bands"]
            self._cache[org_id] = merged

    def get(self, organization_id: str, domain: Optional[str] = None) -> dict[str, Any]:
        if organization_id in self._cache:
            return self._cache[organization_id]
        # Domain-agnostic fallback: still valid org config, no hospital/hotel branching
        cfg = {
            **DEFAULT_ORG_CONFIG,
            "organization_id": organization_id,
            "organization_name": organization_id,
            "domain": domain or "generic",
            "timezone": "UTC",
            "currency": "USD",
        }
        self._cache[organization_id] = cfg
        return cfg


class StreamProcessor:
    def __init__(
        self,
        producer: Any,
        topics: dict[str, str],
        artifact_dir: str = ML_ARTIFACT_DIR,
        org_config_dir: str = ORG_CONFIG_DIR,
    ):
        self.producer = producer
        self.topics = topics
        self.deduper = EventDeduper(ttl_seconds=DEDUPE_TTL_SECONDS)
        self.features_state = FeatureState()
        self.burst = BurstTracker()
        self.orgs = OrgConfigStore(org_config_dir)
        self.artifacts = ArtifactStore(artifact_dir)
        self.artifacts.load()
        self._eps: deque[float] = deque()

    def _send(self, topic: str, key_event: dict[str, Any], value: Any) -> None:
        send_with_retry(self.producer, topic, message_key(key_event), value)

    def _dlq(self, payload: Any, code: str, message: str) -> None:
        envelope = dlq_envelope(self.topics["raw"], code, message, payload)
        org = payload if isinstance(payload, dict) else {}
        try:
            self._send(self.topics["dlq"], org, envelope)
        except Exception:
            events_dropped_total.inc()
            log.exception("DLQ produce failed")
        events_failed_total.inc()

    def process_raw(self, raw: Any, received_at: Optional[datetime] = None) -> Optional[dict[str, Any]]:
        events_received_total.inc()
        received = received_at or utc_now()
        try:
            event_in = validate_event(raw)
            event = normalize_event(event_in)
        except ValidationError as exc:
            self._dlq(raw if isinstance(raw, dict) else {"payload": raw}, exc.code, exc.message)
            return None
        except Exception as exc:  # noqa: BLE001
            self._dlq(raw if isinstance(raw, dict) else {"payload": raw}, "VALIDATION_ERROR", str(exc))
            return None

        if self.deduper.seen(event["organization_id"], event["event_id"]):
            events_deduped_total.inc()
            log.info("deduped %s/%s", event["organization_id"], event["event_id"])
            return None

        org_config = self.orgs.get(event["organization_id"], event.get("domain"))
        baselines: Optional[Baselines] = self.artifacts.baselines_for(event["organization_id"])
        features = build_features(event, org_config, baselines, self.features_state)
        processed_at = utc_now()
        record = {
            "event": event,
            "features": features,
            "processing": {
                "received_at": iso_ms(received),
                "processed_at": iso_ms(processed_at),
                "consumer_id": CONSUMER_ID,
            },
        }

        result_dict = None
        try:
            model = self.artifacts.model_for(event["organization_id"])
            result = score_event(event, features, org_config, baselines, model)
            result_dict = result.to_dict()
            result_dict["features"] = features
            result_dict["timestamp"] = event["timestamp"]
            result_dict["user_id"] = event.get("user_id")
        except Exception as exc:  # noqa: BLE001
            log.exception("ML scoring failed for %s: %s", event["event_id"], exc)
            ml_failures_total.inc()
            service_health.labels(component="ml").set(0)
            self._dlq(event, "ML_ERROR", str(exc))
            # still produce processed event with features; skip anomaly
            try:
                self._send(self.topics["processed"], event, record)
                events_processed_total.inc()
            except Exception:
                events_dropped_total.inc()
                log.exception("processed produce failed after ML error")
            return record

        service_health.labels(component="ml").set(1)
        record["anomaly"] = {
            "anomaly_score": result_dict["anomaly_score"],
            "severity": result_dict["severity"],
            "is_anomaly": result_dict["is_anomaly"],
            "reasons": result_dict.get("reasons") or [],
            "model_version": result_dict.get("model_version"),
        }
        try:
            self._send(self.topics["processed"], event, record)
            events_processed_total.inc()
        except Exception:
            events_dropped_total.inc()
            log.exception("processed produce failed")
            self._dlq(event, "PRODUCE_ERROR", "failed to produce events.processed")
            return None
        latency = (processed_at - received).total_seconds()
        record_processing_latency(latency)

        if result_dict["is_anomaly"]:
            anomalies_total.inc()
            try:
                self._send(self.topics["anomalies"], event, result_dict)
            except Exception:
                log.exception("anomaly produce failed")
        if result_dict["severity"] == "CRITICAL":
            critical_anomalies_total.inc()

        if result_dict["anomaly_score"] >= 0.90:
            alert = make_extreme_alert(event, result_dict)
            try:
                self._send(self.topics["alerts"], event, alert)
                alerts_total.labels(alert_type="SINGLE_EXTREME_EVENT").inc()
            except Exception:
                log.exception("extreme alert produce failed")

        burst_window = self.burst.observe(event, result_dict["anomaly_score"], org_config)
        if burst_window:
            alert = make_burst_alert(event, result_dict, burst_window, org_config)
            try:
                self._send(self.topics["alerts"], event, alert)
                alerts_total.labels(alert_type="SUSPICIOUS_EVENT_BURST").inc()
            except Exception:
                log.exception("burst alert produce failed")

        now = time.time()
        self._eps.append(now)
        while self._eps and now - self._eps[0] > 1.0:
            self._eps.popleft()
        events_per_second.set(len(self._eps))
        return record

    def process_raw_json(self, raw_bytes: bytes) -> Optional[dict[str, Any]]:
        try:
            payload = json.loads(raw_bytes)
        except json.JSONDecodeError as exc:
            self._dlq({"raw": raw_bytes.decode("utf-8", errors="replace")}, "VALIDATION_ERROR", str(exc))
            return None
        return self.process_raw(payload)
