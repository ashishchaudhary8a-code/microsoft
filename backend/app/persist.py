from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy.orm import Session

from backend.app.models import Alert, AnomalyResult, Event, utcnow


def _parse_ts(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        ts = value
    else:
        text = str(value).replace("Z", "+00:00")
        try:
            ts = datetime.fromisoformat(text)
        except ValueError:
            return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def event_to_item(ev: Event, ar: AnomalyResult | None = None) -> dict[str, Any]:
    amount = ev.amount
    if isinstance(amount, Decimal):
        amount = float(amount)
    item = {
        "event_id": ev.event_id,
        "organization_id": ev.organization_id,
        "domain": ev.domain,
        "event_type": ev.event_type,
        "timestamp": ev.timestamp.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") if ev.timestamp else None,
        "user_id": ev.user_id,
        "username": ev.username,
        "location": ev.location,
        "device_id": ev.device_id,
        "ip_address": ev.ip_address,
        "action": ev.action,
        "status": ev.status,
        "amount": amount,
        "currency": ev.currency,
        "anomaly_score": None if ar is None else ar.anomaly_score,
        "severity": None if ar is None else ar.severity,
        "is_anomaly": None if ar is None else ar.is_anomaly,
    }
    return item


def anomaly_list_item(ar: AnomalyResult, ev: Event | None = None, include_reasons: bool = False) -> dict[str, Any]:
    ts = None
    uid = None
    if ev is not None:
        ts = ev.timestamp.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        uid = ev.user_id
    item = {
        "event_id": ar.event_id,
        "organization_id": ar.organization_id,
        "timestamp": ts,
        "user_id": uid,
        "anomaly_score": ar.anomaly_score,
        "severity": ar.severity,
        "is_anomaly": ar.is_anomaly,
        "model_version": ar.model_version,
        "processed_at": ar.processed_at.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
    }
    if include_reasons:
        item["reasons"] = ar.reasons or []
    return item


def alert_item(alert: Alert) -> dict[str, Any]:
    return {
        "alert_id": alert.alert_id,
        "organization_id": alert.organization_id,
        "alert_type": alert.alert_type,
        "severity": alert.severity,
        "user_id": alert.user_id,
        "event_ids": alert.event_ids or [],
        "title": alert.title,
        "summary": alert.summary,
        "created_at": alert.created_at.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
    }


def upsert_processed(session: Session, record: dict[str, Any]) -> tuple[Event, AnomalyResult | None]:
    event = record.get("event") or {}
    features = record.get("features") or {}
    processing = record.get("processing") or {}
    org_id = event["organization_id"]
    event_id = event["event_id"]
    existing = session.get(Event, (org_id, event_id))
    ts = _parse_ts(event["timestamp"])
    received = _parse_ts(processing.get("received_at"))
    fields = dict(
        domain=event.get("domain") or "generic",
        event_type=event.get("event_type") or "event",
        timestamp=ts or utcnow(),
        user_id=event.get("user_id"),
        username=event.get("username"),
        location=event.get("location"),
        device_id=event.get("device_id"),
        ip_address=event.get("ip_address"),
        action=event.get("action"),
        status=event.get("status"),
        amount=event.get("amount"),
        currency=event.get("currency"),
        payload=event.get("payload") or {},
        event_metadata=event.get("metadata") or {},
        received_at=received,
        ingested_at=utcnow(),
    )
    if existing is None:
        existing = Event(organization_id=org_id, event_id=event_id, **fields)
        session.add(existing)
    else:
        for k, v in fields.items():
            setattr(existing, k, v)

    anomaly = record.get("anomaly") or {}
    ar = None
    if anomaly or record.get("is_anomaly") is not None:
        score = anomaly.get("anomaly_score", record.get("anomaly_score"))
        if score is not None:
            ar = upsert_anomaly(
                session,
                {
                    "organization_id": org_id,
                    "event_id": event_id,
                    "anomaly_score": score,
                    "risk_score": anomaly.get("risk_score", score),
                    "severity": anomaly.get("severity") or "NORMAL",
                    "is_anomaly": bool(anomaly.get("is_anomaly", record.get("is_anomaly"))),
                    "reasons": anomaly.get("reasons") or [],
                    "features": features,
                    "model_version": anomaly.get("model_version") or "unknown",
                    "processed_at": processing.get("processed_at") or utcnow().isoformat(),
                    "user_id": event.get("user_id"),
                    "timestamp": event.get("timestamp"),
                },
            )
    session.flush()
    return existing, ar


def upsert_anomaly(session: Session, payload: dict[str, Any]) -> AnomalyResult:
    org_id = payload["organization_id"]
    event_id = payload["event_id"]
    existing = session.get(AnomalyResult, (org_id, event_id))
    processed_at = _parse_ts(payload.get("processed_at")) or utcnow()
    fields = dict(
        anomaly_score=float(payload["anomaly_score"]),
        risk_score=float(payload.get("risk_score") if payload.get("risk_score") is not None else payload["anomaly_score"]),
        severity=payload.get("severity") or "NORMAL",
        is_anomaly=bool(payload.get("is_anomaly")),
        reasons=payload.get("reasons") or [],
        features=payload.get("features"),
        model_version=payload.get("model_version") or "unknown",
        processed_at=processed_at,
    )
    if existing is None:
        existing = AnomalyResult(organization_id=org_id, event_id=event_id, **fields)
        session.add(existing)
    else:
        for k, v in fields.items():
            if k == "features" and v is None:
                continue
            if k == "reasons" and not v and existing.reasons:
                continue
            setattr(existing, k, v)
    session.flush()
    return existing


def insert_alert(session: Session, payload: dict[str, Any]) -> Alert | None:
    alert_id = payload.get("alert_id")
    if not alert_id:
        return None
    existing = session.get(Alert, alert_id)
    if existing is not None:
        return existing
    alert = Alert(
        alert_id=alert_id,
        organization_id=payload["organization_id"],
        alert_type=payload.get("alert_type") or "UNKNOWN",
        severity=payload.get("severity") or "HIGH",
        user_id=payload.get("user_id"),
        event_ids=payload.get("event_ids") or [],
        window_start=_parse_ts(payload.get("window_start")),
        window_end=_parse_ts(payload.get("window_end")),
        title=payload.get("title") or payload.get("alert_type") or "Alert",
        summary=payload.get("summary") or "",
        reasons=payload.get("reasons") or [],
        created_at=_parse_ts(payload.get("created_at")) or utcnow(),
    )
    session.add(alert)
    session.flush()
    return alert
