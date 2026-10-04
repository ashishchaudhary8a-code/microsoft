from __future__ import annotations

from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Deque

# (received_at, visible_at) samples in the last 60s per org
_samples: dict[str, Deque[tuple[float, float]]] = defaultdict(lambda: deque(maxlen=500))
_last_known: dict[str, dict] = {}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def freshness_status(seconds: float | None, warning: float = 5.0, critical: float = 10.0) -> str:
    if seconds is None:
        return "CRITICAL"
    if seconds < warning:
        return "GREEN"
    if seconds <= critical:
        return "WARNING"
    return "CRITICAL"


def record_sample(organization_id: str, received_at: datetime | None, visible_at: datetime | None) -> float | None:
    if received_at is None or visible_at is None:
        return None
    if received_at.tzinfo is None:
        received_at = received_at.replace(tzinfo=timezone.utc)
    if visible_at.tzinfo is None:
        visible_at = visible_at.replace(tzinfo=timezone.utc)
    delta = max((visible_at - received_at).total_seconds(), 0.0)
    now = _now().timestamp()
    dq = _samples[organization_id]
    dq.append((now, delta))
    cutoff = now - 60.0
    while dq and dq[0][0] < cutoff:
        dq.popleft()
    return delta


def _p95(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * 0.95))))
    return float(ordered[idx])


def snapshot(organization_id: str, sla_seconds: float = 5.0) -> dict:
    now = _now().timestamp()
    dq = _samples[organization_id]
    while dq and dq[0][0] < now - 60.0:
        dq.popleft()
    values = [v for _, v in dq]
    latest = values[-1] if values else (_last_known.get(organization_id) or {}).get("freshness_seconds")
    p95 = _p95(values) if values else (_last_known.get(organization_id) or {}).get("freshness_p95_seconds")
    stale = not values
    status = freshness_status(p95 if p95 is not None else latest)
    payload = {
        "organization_id": organization_id,
        "freshness_seconds": None if latest is None else round(float(latest), 1),
        "freshness_p95_seconds": None if p95 is None else round(float(p95), 1),
        "freshness_status": status if not stale or latest is not None else "CRITICAL",
        "sla_seconds": sla_seconds,
        "stale": stale,
    }
    _last_known[organization_id] = payload
    return payload
