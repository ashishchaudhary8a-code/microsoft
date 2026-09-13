"""Basic enrichment / feature prep. Does not call the ML model."""

from __future__ import annotations

from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import Any, Deque, Optional
from zoneinfo import ZoneInfo

from ml.scoring import Baselines
from streaming.app.validate import parse_utc_timestamp


class FeatureState:
    """In-process rolling windows and last-seen timestamps per user."""

    def __init__(self):
        self._windows: dict[tuple[str, str], Deque[datetime]] = defaultdict(deque)
        self._last: dict[tuple[str, str], datetime] = {}

    def observe(self, organization_id: str, user_id: Optional[str], ts: datetime) -> dict[str, Any]:
        if not user_id:
            return {
                "events_last_5min": None,
                "events_last_hour": None,
                "time_since_previous_event_seconds": None,
            }
        key = (organization_id, user_id)
        window = self._windows[key]
        cutoff_hour = ts - timedelta(hours=1)
        while window and window[0] < cutoff_hour:
            window.popleft()
        last = self._last.get(key)
        gap = (ts - last).total_seconds() if last else None
        window.append(ts)
        cutoff_5 = ts - timedelta(minutes=5)
        last_5 = sum(1 for t in window if t >= cutoff_5)
        self._last[key] = ts
        return {
            "events_last_5min": last_5,
            "events_last_hour": len(window),
            "time_since_previous_event_seconds": gap,
        }


def build_features(
    event: dict[str, Any],
    org_config: dict[str, Any],
    baselines: Optional[Baselines],
    state: FeatureState,
) -> dict[str, Any]:
    ts = parse_utc_timestamp(event["timestamp"])
    tz_name = org_config.get("timezone") or "UTC"
    try:
        local_hour = ts.astimezone(ZoneInfo(tz_name)).hour
    except Exception:
        local_hour = ts.hour
    hour_utc = ts.hour
    user_id = event.get("user_id")
    rolling = state.observe(event["organization_id"], user_id, ts)

    thin = False
    ub = None
    if baselines is not None:
        ub, thin = baselines.resolve_user(user_id)
    amount = event.get("amount")
    avg = ub.amount_avg if ub else None
    std = ub.amount_std if ub else None
    zscore = None
    if amount is not None and avg is not None and std not in (None, 0):
        zscore = (float(amount) - float(avg)) / float(std)

    location = event.get("location")
    usual_location = ub.usual_location if ub else None
    distance = None
    if location is not None and usual_location is not None:
        distance = 0 if location == usual_location else 1

    device_id = event.get("device_id")
    new_device = None
    if device_id is not None and ub is not None:
        new_device = device_id not in ub.devices if ub.devices else True

    ip_address = event.get("ip_address")
    new_ip = None
    if ip_address is not None and ub is not None:
        new_ip = ip_address not in ub.ips if ub.ips else True

    unusual_time = None
    if ub is not None and ub.usual_hours:
        unusual_time = hour_utc not in ub.usual_hours and local_hour not in ub.usual_hours

    freq = ub.events_per_day if ub else None
    if thin and baselines is not None:
        # already merged in resolve_user; flag for ML reason
        pass

    return {
        "amount": amount,
        "user_amount_avg": avg,
        "user_amount_std": std,
        "amount_zscore": zscore,
        "events_last_5min": rolling["events_last_5min"],
        "events_last_hour": rolling["events_last_hour"],
        "usual_location": usual_location,
        "distance_from_usual": distance,
        "new_device": new_device,
        "new_ip": new_ip,
        "unusual_time": unusual_time,
        "hour_of_day": hour_utc,
        "hour_of_day_local": local_hour,
        "time_since_previous_event_seconds": rolling["time_since_previous_event_seconds"],
        "user_event_frequency": freq,
        "thin_user_baseline": thin and user_id is not None,
        "device_id": device_id,
        "ip_address": ip_address,
    }
