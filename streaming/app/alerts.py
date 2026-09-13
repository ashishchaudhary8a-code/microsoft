"""Alert emission: single extreme event + suspicious burst (once per window)."""

from __future__ import annotations

import uuid
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import Any, Deque, Optional

from streaming.app.validate import parse_utc_timestamp


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class BurstTracker:
    def __init__(self):
        self._windows: dict[tuple[str, str], Deque[tuple[datetime, str, float]]] = defaultdict(deque)
        self._open: set[tuple[str, str, int]] = set()

    def observe(
        self,
        event: dict[str, Any],
        score: float,
        org_config: dict[str, Any],
    ) -> Optional[list[tuple[datetime, str, float]]]:
        user_id = event.get("user_id")
        if not user_id:
            return None
        threshold = float(org_config.get("suspicious_score_threshold") or 0.60)
        if score < threshold:
            return None
        window_s = int(org_config.get("suspicious_event_window_seconds") or 300)
        need = int(org_config.get("suspicious_event_count") or 3)
        ts = parse_utc_timestamp(event["timestamp"])
        key = (event["organization_id"], user_id)
        dq = self._windows[key]
        cutoff = ts - timedelta(seconds=window_s)
        while dq and dq[0][0] < cutoff:
            dq.popleft()
        dq.append((ts, event["event_id"], score))
        if len(dq) < need:
            return None
        # emit once when threshold first crossed for this open window
        window_id = int(dq[0][0].timestamp())
        token = (event["organization_id"], user_id, window_id)
        if token in self._open:
            return None
        # also collapse if any open token for overlapping first event
        for existing in list(self._open):
            if existing[0] == event["organization_id"] and existing[1] == user_id:
                if abs(existing[2] - window_id) < window_s:
                    return None
        self._open.add(token)
        return list(dq)


def make_extreme_alert(event: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    ts = event["timestamp"]
    return {
        "alert_id": "ALT_" + uuid.uuid4().hex,
        "organization_id": event["organization_id"],
        "alert_type": "SINGLE_EXTREME_EVENT",
        "severity": result.get("severity") or "CRITICAL",
        "user_id": event.get("user_id"),
        "event_ids": [event["event_id"]],
        "window_start": ts,
        "window_end": ts,
        "title": f"Critical anomaly on event {event['event_id']}",
        "summary": f"Single event scored {result['anomaly_score']:.2f} ({result['severity']})",
        "reasons": result.get("reasons") or [],
        "created_at": _now_iso(),
    }


def make_burst_alert(
    event: dict[str, Any],
    result: dict[str, Any],
    window: list[tuple[datetime, str, float]],
    org_config: dict[str, Any],
) -> dict[str, Any]:
    event_ids = [item[1] for item in window]
    start = window[0][0]
    end = window[-1][0]
    need = int(org_config.get("suspicious_event_count") or 3)
    win = int(org_config.get("suspicious_event_window_seconds") or 300)
    return {
        "alert_id": "ALT_" + uuid.uuid4().hex,
        "organization_id": event["organization_id"],
        "alert_type": "SUSPICIOUS_EVENT_BURST",
        "severity": result.get("severity") or "HIGH",
        "user_id": event.get("user_id"),
        "event_ids": event_ids,
        "window_start": _iso(start),
        "window_end": _iso(end),
        "title": f"Suspicious burst for user {event.get('user_id')}",
        "summary": f"{len(event_ids)} suspicious events (need {need}) within {win}s",
        "reasons": result.get("reasons") or [],
        "created_at": _now_iso(),
    }
