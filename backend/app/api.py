from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Optional

import httpx
from fastapi import APIRouter, Depends, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, Response
from sqlalchemy.orm import Session

from backend.app.config import KAFKA_BOOTSTRAP_SERVERS, STREAMING_HEALTH_URL
from backend.app.db import get_session, ping_db
from backend.app.freshness import snapshot as freshness_snapshot
from backend.app.metrics import metrics_response, set_component_health
from backend.app.models import Alert, AnomalyResult, Event, Organization
from backend.app.persist import alert_item, anomaly_list_item, event_to_item
from backend.app.queries import dashboard_kpis, last_event_times, list_alerts, list_anomalies, list_events
from backend.app.ws import hub

log = logging.getLogger("backend.api")
router = APIRouter()


def error_body(code: str, message: str, status: int) -> JSONResponse:
    return JSONResponse(status_code=status, content={"error": {"code": code, "message": message}})


def require_org(organization_id: Optional[str]) -> Optional[JSONResponse]:
    if not organization_id:
        return error_body("MISSING_ORGANIZATION", "organization_id is required", 400)
    return None


def resolve_org_id(request: Request, organization_id: Optional[str]) -> Optional[str]:
    if organization_id:
        return organization_id
    header = request.headers.get("x-organization-id")
    return header or None


def clamp_limit(limit: Optional[int]) -> int:
    if limit is None:
        return 50
    return max(1, min(int(limit), 200))


def probe_streaming() -> dict:
    if os.environ.get("BACKEND_SKIP_BROKER_PROBE") == "1":
        return {"streaming": "unknown", "broker": "unknown", "ml": "unknown", "consumer_lag": 0}
    try:
        with httpx.Client(timeout=2.0) as client:
            r = client.get(STREAMING_HEALTH_URL)
            data = r.json()
            return {
                "streaming": data.get("streaming") or data.get("status") or "unknown",
                "broker": data.get("broker") or "unknown",
                "ml": data.get("ml") or "unknown",
                "consumer_lag": data.get("consumer_lag", 0),
            }
    except Exception:
        return {"streaming": "down", "broker": "unknown", "ml": "unknown", "consumer_lag": 0}


def probe_broker() -> str:
    if os.environ.get("BACKEND_SKIP_BROKER_PROBE") == "1":
        return "unknown"
    try:
        from kafka import KafkaAdminClient

        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","), request_timeout_ms=2000)
        admin.close()
        return "ok"
    except Exception:
        try:
            from kafka import KafkaProducer

            p = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","), request_timeout_ms=2000)
            p.close()
            return "ok"
        except Exception:
            return "down"


@router.get("/api/health")
def health():
    db_ok = ping_db()
    stream = probe_streaming()
    broker = stream.get("broker") if stream.get("broker") not in (None, "unknown") else probe_broker()
    if stream.get("broker") == "unknown" and broker == "ok":
        pass
    if stream.get("streaming") == "down" and broker == "down":
        broker = "down"
    backend = "ok" if db_ok else "degraded"
    components = {
        "backend": backend,
        "database": "ok" if db_ok else "down",
        "broker": broker if broker in ("ok", "degraded", "down") else "unknown",
        "streaming": stream.get("streaming") or "unknown",
        "ml": stream.get("ml") or "unknown",
    }
    status = "ok" if all(v == "ok" for v in components.values()) else "degraded"
    set_component_health("backend", backend == "ok")
    set_component_health("db", db_ok)
    set_component_health("broker", components["broker"] == "ok")
    set_component_health("streaming", components["streaming"] == "ok")
    set_component_health("ml", components["ml"] == "ok")
    return {
        "status": status,
        **components,
        "checked_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "consumer_lag": stream.get("consumer_lag", 0),
        "websocket_clients": hub.client_count(),
    }


@router.get("/metrics")
def metrics():
    body, ctype = metrics_response()
    return Response(content=body, media_type=ctype)


@router.get("/api/organizations")
def organizations(session: Session = Depends(get_session)):
    rows = session.query(Organization).order_by(Organization.organization_id).all()
    return {
        "organizations": [
            {
                "organization_id": o.organization_id,
                "organization_name": o.organization_name,
                "domain": o.domain,
                "timezone": o.timezone,
                "currency": o.currency,
            }
            for o in rows
        ]
    }


@router.get("/api/dashboard")
def dashboard(
    request: Request,
    organization_id: Optional[str] = None,
    session: Session = Depends(get_session),
):
    organization_id = resolve_org_id(request, organization_id)
    missing = require_org(organization_id)
    if missing:
        return missing
    org = session.get(Organization, organization_id)
    sla = 5.0
    if org and isinstance(org.config, dict):
        sla = float(org.config.get("freshness_sla_seconds") or 5)
    kpis = dashboard_kpis(session, organization_id)
    snap = freshness_snapshot(organization_id, sla)
    pipeline = "ok"
    if snap.get("stale"):
        pipeline = "stale"
    stream = probe_streaming()
    if stream.get("streaming") != "ok":
        pipeline = stream.get("streaming") or "degraded"
    return {
        "organization_id": organization_id,
        **kpis,
        "freshness_seconds": snap.get("freshness_seconds"),
        "freshness_status": snap.get("freshness_status"),
        "pipeline_status": pipeline,
    }


@router.get("/api/events")
def events(
    request: Request,
    organization_id: Optional[str] = None,
    limit: Optional[int] = Query(default=50),
    cursor: Optional[str] = None,
    user_id: Optional[str] = None,
    session: Session = Depends(get_session),
):
    organization_id = resolve_org_id(request, organization_id)
    missing = require_org(organization_id)
    if missing:
        return missing
    rows, next_cursor = list_events(session, organization_id, clamp_limit(limit), cursor, user_id)
    return {"items": [event_to_item(ev, ar) for ev, ar in rows], "next_cursor": next_cursor}


@router.get("/api/anomalies")
def anomalies(
    request: Request,
    organization_id: Optional[str] = None,
    limit: Optional[int] = Query(default=50),
    cursor: Optional[str] = None,
    severity: Optional[str] = None,
    user_id: Optional[str] = None,
    session: Session = Depends(get_session),
):
    organization_id = resolve_org_id(request, organization_id)
    missing = require_org(organization_id)
    if missing:
        return missing
    rows, next_cursor = list_anomalies(session, organization_id, clamp_limit(limit), cursor, severity, user_id)
    return {
        "items": [anomaly_list_item(ar, ev) for ar, ev in rows],
        "next_cursor": next_cursor,
    }


@router.get("/api/anomalies/{event_id}")
def anomaly_detail(
    event_id: str,
    request: Request,
    organization_id: Optional[str] = None,
    session: Session = Depends(get_session),
):
    organization_id = resolve_org_id(request, organization_id)
    missing = require_org(organization_id)
    if missing:
        return missing
    ev = session.get(Event, (organization_id, event_id))
    ar = session.get(AnomalyResult, (organization_id, event_id))
    if ev is None and ar is None:
        return error_body("NOT_FOUND", "Anomaly not found", 404)
    alerts = (
        session.query(Alert)
        .filter(Alert.organization_id == organization_id)
        .order_by(Alert.created_at.desc())
        .all()
    )
    linked = [alert_item(a) for a in alerts if event_id in (a.event_ids or [])]
    event_payload = None
    if ev is not None:
        event_payload = {
            "event_id": ev.event_id,
            "organization_id": ev.organization_id,
            "domain": ev.domain,
            "event_type": ev.event_type,
            "timestamp": ev.timestamp.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "user_id": ev.user_id,
            "username": ev.username,
            "location": ev.location,
            "device_id": ev.device_id,
            "ip_address": ev.ip_address,
            "action": ev.action,
            "status": ev.status,
            "amount": float(ev.amount) if ev.amount is not None else None,
            "currency": ev.currency,
            "payload": ev.payload or {},
            "metadata": ev.event_metadata or {},
        }
    return {
        "event_id": event_id,
        "organization_id": organization_id,
        "event": event_payload or {},
        "features": (ar.features if ar else None) or {},
        "anomaly_score": None if ar is None else ar.anomaly_score,
        "risk_score": None if ar is None else ar.risk_score,
        "severity": None if ar is None else ar.severity,
        "is_anomaly": None if ar is None else ar.is_anomaly,
        "reasons": [] if ar is None else (ar.reasons or []),
        "model_version": None if ar is None else ar.model_version,
        "processed_at": None
        if ar is None
        else ar.processed_at.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "alerts": [{"alert_id": a["alert_id"], "alert_type": a["alert_type"], "severity": a["severity"]} for a in linked],
    }


@router.get("/api/alerts")
def alerts(
    request: Request,
    organization_id: Optional[str] = None,
    limit: Optional[int] = Query(default=50),
    cursor: Optional[str] = None,
    alert_type: Optional[str] = None,
    session: Session = Depends(get_session),
):
    organization_id = resolve_org_id(request, organization_id)
    missing = require_org(organization_id)
    if missing:
        return missing
    rows, next_cursor = list_alerts(session, organization_id, clamp_limit(limit), cursor, alert_type)
    return {"items": [alert_item(a) for a in rows], "next_cursor": next_cursor}


@router.get("/api/pipeline/freshness")
def pipeline_freshness(
    request: Request,
    organization_id: Optional[str] = None,
    session: Session = Depends(get_session),
):
    organization_id = resolve_org_id(request, organization_id)
    missing = require_org(organization_id)
    if missing:
        return missing
    org = session.get(Organization, organization_id)
    sla = 5.0
    if org and isinstance(org.config, dict):
        sla = float(org.config.get("freshness_sla_seconds") or 5)
    snap = freshness_snapshot(organization_id, sla)
    last_ts, last_ingested = last_event_times(session, organization_id)
    stream = probe_streaming()
    pipeline = "ok"
    if snap.get("stale"):
        pipeline = "stale"
    if stream.get("streaming") != "ok":
        pipeline = stream.get("streaming") or "degraded"
    def fmt(ts):
        if ts is None:
            return None
        return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    return {
        "organization_id": organization_id,
        "freshness_seconds": snap.get("freshness_seconds"),
        "freshness_p95_seconds": snap.get("freshness_p95_seconds"),
        "freshness_status": snap.get("freshness_status"),
        "sla_seconds": sla,
        "last_event_timestamp": fmt(last_ts),
        "last_dashboard_visible_at": fmt(last_ingested),
        "pipeline_status": pipeline,
    }


@router.websocket("/ws/live")
async def ws_live(websocket: WebSocket, organization_id: Optional[str] = None):
    organization_id = organization_id or websocket.query_params.get("organization_id")
    if not organization_id:
        await websocket.close(code=1008)
        return
    await hub.connect(organization_id, websocket)
    try:
        while True:
            # listen-only; still drain optional pings
            await websocket.receive_text()
    except WebSocketDisconnect:
        await hub.disconnect(organization_id, websocket)
    except Exception:
        await hub.disconnect(organization_id, websocket)
