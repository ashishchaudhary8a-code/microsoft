from __future__ import annotations

import base64
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import and_, desc, func, select
from sqlalchemy.orm import Session

from backend.app.models import Alert, AnomalyResult, Event, Organization


def encode_cursor(ts: datetime, event_id: str) -> str:
    raw = f"{ts.astimezone(timezone.utc).isoformat()}|{event_id}"
    return base64.urlsafe_b64encode(raw.encode()).decode()


def decode_cursor(cursor: str | None) -> tuple[datetime, str] | None:
    if not cursor:
        return None
    try:
        raw = base64.urlsafe_b64decode(cursor.encode()).decode()
        ts_s, event_id = raw.split("|", 1)
        ts = datetime.fromisoformat(ts_s.replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts, event_id
    except Exception:
        return None


def org_or_none(session: Session, organization_id: str) -> Organization | None:
    return session.get(Organization, organization_id)


def list_events(
    session: Session,
    organization_id: str,
    limit: int,
    cursor: str | None,
    user_id: str | None,
) -> tuple[list[tuple[Event, AnomalyResult | None]], str | None]:
    stmt = select(Event, AnomalyResult).outerjoin(
        AnomalyResult,
        and_(
            AnomalyResult.organization_id == Event.organization_id,
            AnomalyResult.event_id == Event.event_id,
        ),
    ).where(Event.organization_id == organization_id)
    if user_id:
        stmt = stmt.where(Event.user_id == user_id)
    decoded = decode_cursor(cursor)
    if decoded:
        ts, eid = decoded
        stmt = stmt.where(
            (Event.timestamp < ts) | and_(Event.timestamp == ts, Event.event_id < eid)
        )
    stmt = stmt.order_by(desc(Event.timestamp), desc(Event.event_id)).limit(limit + 1)
    rows = list(session.execute(stmt).all())
    next_cursor = None
    if len(rows) > limit:
        last_ev = rows[limit - 1][0]
        next_cursor = encode_cursor(last_ev.timestamp, last_ev.event_id)
        rows = rows[:limit]
    return rows, next_cursor


def list_anomalies(
    session: Session,
    organization_id: str,
    limit: int,
    cursor: str | None,
    severity: str | None,
    user_id: str | None,
) -> tuple[list[tuple[AnomalyResult, Event | None]], str | None]:
    stmt = (
        select(AnomalyResult, Event)
        .outerjoin(
            Event,
            and_(
                Event.organization_id == AnomalyResult.organization_id,
                Event.event_id == AnomalyResult.event_id,
            ),
        )
        .where(
            AnomalyResult.organization_id == organization_id,
            AnomalyResult.is_anomaly.is_(True),
        )
    )
    if severity:
        stmt = stmt.where(AnomalyResult.severity == severity)
    if user_id:
        stmt = stmt.where(Event.user_id == user_id)
    decoded = decode_cursor(cursor)
    if decoded:
        ts, eid = decoded
        stmt = stmt.where(
            (AnomalyResult.processed_at < ts)
            | and_(AnomalyResult.processed_at == ts, AnomalyResult.event_id < eid)
        )
    stmt = stmt.order_by(desc(AnomalyResult.processed_at), desc(AnomalyResult.event_id)).limit(limit + 1)
    rows = list(session.execute(stmt).all())
    next_cursor = None
    if len(rows) > limit:
        last = rows[limit - 1][0]
        next_cursor = encode_cursor(last.processed_at, last.event_id)
        rows = rows[:limit]
    return rows, next_cursor


def list_alerts(
    session: Session,
    organization_id: str,
    limit: int,
    cursor: str | None,
    alert_type: str | None,
) -> tuple[list[Alert], str | None]:
    stmt = select(Alert).where(Alert.organization_id == organization_id)
    if alert_type:
        stmt = stmt.where(Alert.alert_type == alert_type)
    decoded = decode_cursor(cursor)
    if decoded:
        ts, aid = decoded
        stmt = stmt.where((Alert.created_at < ts) | and_(Alert.created_at == ts, Alert.alert_id < aid))
    stmt = stmt.order_by(desc(Alert.created_at), desc(Alert.alert_id)).limit(limit + 1)
    rows = list(session.execute(stmt).scalars().all())
    next_cursor = None
    if len(rows) > limit:
        last = rows[limit - 1]
        next_cursor = encode_cursor(last.created_at, last.alert_id)
        rows = rows[:limit]
    return rows, next_cursor


def dashboard_kpis(session: Session, organization_id: str, window_seconds: int = 60) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    # window for epm uses ingested_at / timestamp last 60s
    from datetime import timedelta

    cutoff = now - timedelta(seconds=window_seconds)
    events_total = session.scalar(
        select(func.count()).select_from(Event).where(Event.organization_id == organization_id)
    ) or 0
    recent = session.scalar(
        select(func.count())
        .select_from(Event)
        .where(Event.organization_id == organization_id, Event.ingested_at >= cutoff)
    ) or 0
    anomalies_total = session.scalar(
        select(func.count())
        .select_from(AnomalyResult)
        .where(AnomalyResult.organization_id == organization_id, AnomalyResult.is_anomaly.is_(True))
    ) or 0
    critical_total = session.scalar(
        select(func.count())
        .select_from(AnomalyResult)
        .where(
            AnomalyResult.organization_id == organization_id,
            AnomalyResult.severity == "CRITICAL",
        )
    ) or 0
    avg_score = session.scalar(
        select(func.avg(AnomalyResult.anomaly_score)).where(AnomalyResult.organization_id == organization_id)
    )
    return {
        "events_total": int(events_total),
        "events_per_minute": float(recent) * (60.0 / window_seconds),
        "anomalies_total": int(anomalies_total),
        "critical_total": int(critical_total),
        "anomaly_score_avg": None if avg_score is None else round(float(avg_score), 4),
        "window_seconds": window_seconds,
    }


def last_event_times(session: Session, organization_id: str) -> tuple[datetime | None, datetime | None]:
    row = session.execute(
        select(Event.timestamp, Event.ingested_at)
        .where(Event.organization_id == organization_id)
        .order_by(desc(Event.ingested_at))
        .limit(1)
    ).first()
    if not row:
        return None, None
    return row[0], row[1]
