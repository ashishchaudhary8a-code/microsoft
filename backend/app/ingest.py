from __future__ import annotations

import json
import logging
import threading
import time
from typing import Any, Optional

from sqlalchemy.orm import Session

from backend.app.config import (
    BACKEND_CONSUMER_GROUP,
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_ALERTS,
    TOPIC_ANOMALIES,
    TOPIC_PROCESSED,
)
from backend.app.db import get_session_factory
from backend.app.freshness import record_sample, snapshot as freshness_snapshot
from backend.app.metrics import freshness_p95_seconds, freshness_seconds, set_component_health
from backend.app.persist import (
    alert_item,
    anomaly_list_item,
    event_to_item,
    insert_alert,
    upsert_anomaly,
    upsert_processed,
)
from backend.app.queries import dashboard_kpis
from backend.app.ws import emit_alert, emit_anomaly, emit_dashboard, emit_event, emit_freshness, hub

log = logging.getLogger("backend.ingest")

_loop = None
STOP = threading.Event()


def attach_loop(loop) -> None:
    global _loop
    _loop = loop


def _run(coro) -> None:
    if _loop is None:
        return
    fut = __import__("asyncio").run_coroutine_threadsafe(coro, _loop)
    try:
        fut.result(timeout=5)
    except Exception:
        log.debug("ws emit failed", exc_info=True)


def _decode(raw: bytes | None) -> Optional[dict[str, Any]]:
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        log.warning("invalid json on backend ingest")
        return None
    return payload if isinstance(payload, dict) else None


def _sla(session: Session, org_id: str) -> float:
    from backend.app.models import Organization

    org = session.get(Organization, org_id)
    if org and isinstance(org.config, dict):
        return float(org.config.get("freshness_sla_seconds") or 5)
    return 5.0


def persist_processed(session: Session, record: dict[str, Any]) -> None:
    event, ar = upsert_processed(session, record)
    session.commit()
    org_id = event.organization_id
    from datetime import datetime, timezone

    visible = datetime.now(timezone.utc)
    delta = record_sample(org_id, event.received_at, visible)
    snap = freshness_snapshot(org_id, _sla(session, org_id))
    if snap.get("freshness_seconds") is not None:
        freshness_seconds.labels(organization_id=org_id).set(snap["freshness_seconds"])
    if snap.get("freshness_p95_seconds") is not None:
        freshness_p95_seconds.labels(organization_id=org_id).set(snap["freshness_p95_seconds"])
    item = event_to_item(event, ar)
    _run(emit_event(org_id, item))
    if ar is not None and ar.is_anomaly:
        _run(emit_anomaly(org_id, anomaly_list_item(ar, event, include_reasons=True)))
    kpis = dashboard_kpis(session, org_id)
    kpis.update(
        {
            "organization_id": org_id,
            "freshness_seconds": snap.get("freshness_seconds"),
            "freshness_status": snap.get("freshness_status"),
            "pipeline_status": "ok" if not snap.get("stale") else "stale",
        }
    )
    _run(emit_dashboard(org_id, kpis))
    _run(emit_freshness(org_id, _sla(session, org_id)))
    del delta


def persist_anomaly_msg(session: Session, payload: dict[str, Any]) -> None:
    ar = upsert_anomaly(session, payload)
    session.commit()
    from backend.app.models import Event

    ev = session.get(Event, (ar.organization_id, ar.event_id))
    if ar.is_anomaly:
        _run(emit_anomaly(ar.organization_id, anomaly_list_item(ar, ev, include_reasons=True)))


def persist_alert_msg(session: Session, payload: dict[str, Any]) -> None:
    alert = insert_alert(session, payload)
    session.commit()
    if alert:
        _run(emit_alert(alert.organization_id, alert_item(alert)))


def handle_message(topic: str, value: bytes) -> bool:
    payload = _decode(value)
    if payload is None:
        return True  # skip poison json; do not retry forever
    session = get_session_factory()()
    try:
        if topic == TOPIC_PROCESSED:
            if "event" not in payload:
                return True
            persist_processed(session, payload)
        elif topic == TOPIC_ANOMALIES:
            persist_anomaly_msg(session, payload)
        elif topic == TOPIC_ALERTS:
            persist_alert_msg(session, payload)
        return True
    except Exception:
        session.rollback()
        log.exception("persist failed topic=%s", topic)
        return False
    finally:
        session.close()


def consume_loop() -> None:
    from kafka import KafkaConsumer
    from kafka.errors import KafkaError, NoBrokersAvailable

    bootstrap = KAFKA_BOOTSTRAP_SERVERS
    while not STOP.is_set():
        consumer = None
        try:
            consumer = KafkaConsumer(
                TOPIC_PROCESSED,
                TOPIC_ANOMALIES,
                TOPIC_ALERTS,
                bootstrap_servers=bootstrap.split(","),
                group_id=BACKEND_CONSUMER_GROUP,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                value_deserializer=lambda v: v,
                consumer_timeout_ms=1000,
            )
            set_component_health("backend", True)
            log.info("backend consuming processed/anomaly/alert from %s", bootstrap)
            while not STOP.is_set():
                records = consumer.poll(timeout_ms=500, max_records=50)
                if not records:
                    continue
                ok = True
                for tp, batch in records.items():
                    for msg in batch:
                        if not handle_message(tp.topic, msg.value):
                            ok = False
                            break
                    if not ok:
                        break
                if ok:
                    consumer.commit()
                else:
                    time.sleep(1)
                    break
        except NoBrokersAvailable:
            set_component_health("backend", False)
            log.warning("broker unavailable; backend ingest retry")
            time.sleep(2)
        except KafkaError:
            set_component_health("backend", False)
            log.exception("kafka error in backend ingest")
            time.sleep(2)
        except Exception:
            log.exception("backend ingest crashed; retrying")
            time.sleep(2)
        finally:
            if consumer is not None:
                try:
                    consumer.close()
                except Exception:
                    pass


def start_ingest_thread() -> threading.Thread:
    STOP.clear()
    t = threading.Thread(target=consume_loop, name="backend-ingest", daemon=True)
    t.start()
    return t
