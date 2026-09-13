"""ML scoring: baselines, Isolation Forest blend, explainable reasons.

Streaming calls score_event. Training is batch-only (see train.py).
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable, Optional

MODEL_VERSION = "baseline-iforest-v1"
MODEL_VERSION_BASELINE_ONLY = "baseline-only-v1"
THIN_USER_THRESHOLD = 20

DEFAULT_SEVERITY_BANDS = {
    "NORMAL": {"min": 0.00, "max": 0.39},
    "LOW": {"min": 0.40, "max": 0.59},
    "MEDIUM": {"min": 0.60, "max": 0.74},
    "HIGH": {"min": 0.75, "max": 0.89},
    "CRITICAL": {"min": 0.90, "max": 1.00},
}

CONTEXTUAL_RULES = (
    ("new_device", "NEW_DEVICE", "Device {device_id} not seen for this user"),
    ("new_ip", "NEW_IP", "IP {ip_address} not seen for this user"),
    ("unusual_time", "UNUSUAL_TIME", "Event hour {hour_of_day} is outside usual hours"),
    ("distance_from_usual", "LOCATION_CHANGE", "Location differs from usual {usual_location}"),
)


def clamp01(value: float) -> float:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return 0.0
    return max(0.0, min(1.0, float(value)))


def severity_from_score(score: float, bands: Optional[dict] = None) -> str:
    """Boundary values use the higher severity (0.40 → LOW, 1.00 → CRITICAL)."""
    bands = bands or DEFAULT_SEVERITY_BANDS
    s = clamp01(score)
    order = ("CRITICAL", "HIGH", "MEDIUM", "LOW", "NORMAL")
    for name in order:
        band = bands.get(name) or DEFAULT_SEVERITY_BANDS[name]
        if s >= float(band["min"]):
            return name
    return "NORMAL"


def squash_zscore(z: Optional[float]) -> float:
    if z is None:
        return 0.0
    return clamp01(math.tanh(abs(float(z)) / 3.0))


@dataclass
class Reason:
    code: str
    feature: str
    detail: str
    contribution: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "feature": self.feature,
            "detail": self.detail,
            "contribution": round(self.contribution, 4),
        }


@dataclass
class AnomalyResult:
    event_id: str
    organization_id: str
    anomaly_score: float
    risk_score: float
    severity: str
    is_anomaly: bool
    reasons: list[Reason]
    model_version: str
    processed_at: str
    baseline_score: float = 0.0
    contextual_score: float = 0.0
    iforest_score: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "organization_id": self.organization_id,
            "anomaly_score": round(self.anomaly_score, 4),
            "risk_score": round(self.risk_score, 4),
            "severity": self.severity,
            "is_anomaly": self.is_anomaly,
            "reasons": [r.to_dict() for r in self.reasons],
            "model_version": self.model_version,
            "processed_at": self.processed_at,
        }


@dataclass
class UserBaseline:
    amount_avg: Optional[float] = None
    amount_std: Optional[float] = None
    usual_location: Optional[str] = None
    devices: set[str] = field(default_factory=set)
    ips: set[str] = field(default_factory=set)
    usual_hours: set[int] = field(default_factory=set)
    event_count: int = 0
    events_per_day: float = 0.0

    def to_json(self) -> dict[str, Any]:
        return {
            "amount_avg": self.amount_avg,
            "amount_std": self.amount_std,
            "usual_location": self.usual_location,
            "devices": sorted(self.devices),
            "ips": sorted(self.ips),
            "usual_hours": sorted(self.usual_hours),
            "event_count": self.event_count,
            "events_per_day": self.events_per_day,
        }

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> "UserBaseline":
        return cls(
            amount_avg=data.get("amount_avg"),
            amount_std=data.get("amount_std"),
            usual_location=data.get("usual_location"),
            devices=set(data.get("devices") or []),
            ips=set(data.get("ips") or []),
            usual_hours=set(data.get("usual_hours") or []),
            event_count=int(data.get("event_count") or 0),
            events_per_day=float(data.get("events_per_day") or 0.0),
        )


@dataclass
class Baselines:
    organization_id: str
    org: UserBaseline = field(default_factory=UserBaseline)
    users: dict[str, UserBaseline] = field(default_factory=dict)
    lookback_days: int = 30

    def resolve_user(self, user_id: Optional[str]) -> tuple[UserBaseline, bool]:
        if not user_id or user_id not in self.users:
            return self.org, True
        ub = self.users[user_id]
        if ub.event_count < THIN_USER_THRESHOLD:
            merged = UserBaseline(
                amount_avg=ub.amount_avg if ub.amount_avg is not None else self.org.amount_avg,
                amount_std=ub.amount_std if ub.amount_std is not None else self.org.amount_std,
                usual_location=ub.usual_location or self.org.usual_location,
                devices=ub.devices or self.org.devices,
                ips=ub.ips or self.org.ips,
                usual_hours=ub.usual_hours or self.org.usual_hours,
                event_count=ub.event_count,
                events_per_day=ub.events_per_day or self.org.events_per_day,
            )
            return merged, True
        return ub, False

    def to_json(self) -> dict[str, Any]:
        return {
            "organization_id": self.organization_id,
            "lookback_days": self.lookback_days,
            "org": self.org.to_json(),
            "users": {k: v.to_json() for k, v in self.users.items()},
        }

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> "Baselines":
        users = {k: UserBaseline.from_json(v) for k, v in (data.get("users") or {}).items()}
        return cls(
            organization_id=data["organization_id"],
            org=UserBaseline.from_json(data.get("org") or {}),
            users=users,
            lookback_days=int(data.get("lookback_days") or 30),
        )


def _parse_ts(value: Any) -> Optional[datetime]:
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


def _mean_std(values: list[float]) -> tuple[Optional[float], Optional[float]]:
    if not values:
        return None, None
    mean = sum(values) / len(values)
    if len(values) == 1:
        return mean, 0.0
    var = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
    return mean, math.sqrt(var)


def _mode(values: list[str]) -> Optional[str]:
    if not values:
        return None
    counts: dict[str, int] = {}
    for v in values:
        counts[v] = counts.get(v, 0) + 1
    return max(counts.items(), key=lambda kv: kv[1])[0]


def fit_baselines(historical_events: Iterable[dict[str, Any]], org_config: dict[str, Any]) -> Baselines:
    org_id = org_config["organization_id"]
    lookback = int(org_config.get("baseline_lookback_days") or 30)
    by_user: dict[str, list[dict[str, Any]]] = {}
    all_events: list[dict[str, Any]] = []
    for ev in historical_events:
        if ev.get("organization_id") and ev["organization_id"] != org_id:
            continue
        all_events.append(ev)
        uid = ev.get("user_id")
        if uid:
            by_user.setdefault(uid, []).append(ev)

    def build(events: list[dict[str, Any]]) -> UserBaseline:
        amounts = [float(e["amount"]) for e in events if e.get("amount") is not None]
        avg, std = _mean_std(amounts)
        locations = [str(e["location"]) for e in events if e.get("location")]
        devices = {str(e["device_id"]) for e in events if e.get("device_id")}
        ips = {str(e["ip_address"]) for e in events if e.get("ip_address")}
        hours: list[int] = []
        timestamps: list[datetime] = []
        for e in events:
            ts = _parse_ts(e.get("timestamp"))
            if ts:
                hours.append(ts.hour)
                timestamps.append(ts)
        usual_hours = set()
        if hours:
            # keep hours that appear at least once; "usual" = hours at/above median frequency
            freq: dict[int, int] = {}
            for h in hours:
                freq[h] = freq.get(h, 0) + 1
            median = sorted(freq.values())[len(freq) // 2]
            usual_hours = {h for h, c in freq.items() if c >= max(1, median // 2)}
        span_days = lookback
        if timestamps:
            span = (max(timestamps) - min(timestamps)).total_seconds() / 86400.0
            span_days = max(span, 1.0)
        return UserBaseline(
            amount_avg=avg,
            amount_std=std if std and std > 0 else (avg * 0.25 if avg else None),
            usual_location=_mode(locations),
            devices=devices,
            ips=ips,
            usual_hours=usual_hours,
            event_count=len(events),
            events_per_day=len(events) / span_days if span_days else float(len(events)),
        )

    org_base = build(all_events)
    users = {uid: build(evs) for uid, evs in by_user.items()}
    return Baselines(organization_id=org_id, org=org_base, users=users, lookback_days=lookback)


NUMERIC_FEATURE_KEYS = (
    "amount",
    "user_amount_avg",
    "user_amount_std",
    "amount_zscore",
    "events_last_5min",
    "events_last_hour",
    "distance_from_usual",
    "new_device",
    "new_ip",
    "unusual_time",
    "hour_of_day",
    "time_since_previous_event_seconds",
    "user_event_frequency",
)


def feature_vector(features: dict[str, Any]) -> list[float]:
    row: list[float] = []
    for key in NUMERIC_FEATURE_KEYS:
        val = features.get(key)
        if val is None:
            row.append(0.0)
        elif isinstance(val, bool):
            row.append(1.0 if val else 0.0)
        else:
            try:
                row.append(float(val))
            except (TypeError, ValueError):
                row.append(0.0)
    return row


def fit_model(feature_matrix: list[list[float]], org_config: dict[str, Any]):
    """Train Isolation Forest. Returns a sklearn estimator or None if not enough rows."""
    del org_config  # unused; kept for frozen interface
    try:
        from sklearn.ensemble import IsolationForest
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("scikit-learn is required to fit Isolation Forest") from exc
    if not feature_matrix or len(feature_matrix) < 10:
        return None
    model = IsolationForest(
        n_estimators=100,
        contamination=0.05,
        random_state=42,
        n_jobs=1,
    )
    model.fit(feature_matrix)
    return model


def _iforest_score(model: Any, features: dict[str, Any]) -> float:
    if model is None:
        return 0.0
    vec = [feature_vector(features)]
    raw = float(model.decision_function(vec)[0])
    # sklearn: more negative => more anomalous. Map typical range ~[-0.5, 0.5] to [0,1]
    return clamp01(0.5 - raw)


def _contextual_score(features: dict[str, Any]) -> tuple[float, list[Reason]]:
    fired = 0
    applicable = 0
    reasons: list[Reason] = []
    for feature_name, code, template in CONTEXTUAL_RULES:
        val = features.get(feature_name)
        if val is None:
            continue
        applicable += 1
        active = bool(val) if not isinstance(val, (int, float)) else float(val) > 0
        if not active:
            continue
        fired += 1
        detail = template.format(
            device_id=features.get("device_id") or "unknown",
            ip_address=features.get("ip_address") or "unknown",
            hour_of_day=features.get("hour_of_day"),
            usual_location=features.get("usual_location") or "unknown",
        )
        reasons.append(Reason(code=code, feature=feature_name, detail=detail, contribution=0.0))

    z = features.get("amount_zscore")
    if z is not None and abs(float(z)) >= 3:
        applicable += 1
        fired += 1
        reasons.append(
            Reason(
                code="AMOUNT_ZSCORE",
                feature="amount_zscore",
                detail=(
                    f"Amount {features.get('amount')} is {float(z):.1f} std above "
                    f"user mean {features.get('user_amount_avg')}"
                ),
                contribution=0.0,
            )
        )
    elif z is not None:
        applicable += 1

    v5 = features.get("events_last_5min")
    if v5 is not None and float(v5) >= 5:
        applicable += 1
        fired += 1
        reasons.append(
            Reason(
                code="HIGH_VELOCITY_5MIN",
                feature="events_last_5min",
                detail=f"{int(v5)} events in the last 5 minutes for this user",
                contribution=0.0,
            )
        )
    elif v5 is not None:
        applicable += 1

    vh = features.get("events_last_hour")
    if vh is not None and float(vh) >= 20:
        applicable += 1
        fired += 1
        reasons.append(
            Reason(
                code="HIGH_VELOCITY_HOUR",
                feature="events_last_hour",
                detail=f"{int(vh)} events in the last hour for this user",
                contribution=0.0,
            )
        )
    elif vh is not None:
        applicable += 1

    gap = features.get("time_since_previous_event_seconds")
    if gap is not None and float(gap) >= 0 and float(gap) < 5:
        applicable += 1
        fired += 1
        reasons.append(
            Reason(
                code="SHORT_INTERVAL",
                feature="time_since_previous_event_seconds",
                detail=f"Only {float(gap):.1f}s since previous event",
                contribution=0.0,
            )
        )
    elif gap is not None:
        applicable += 1

    score = (fired / applicable) if applicable else 0.0
    return clamp01(score), reasons


def _baseline_score(features: dict[str, Any]) -> float:
    z_part = squash_zscore(features.get("amount_zscore"))
    freq = features.get("user_event_frequency")
    v5 = features.get("events_last_5min")
    freq_part = 0.0
    if freq is not None and v5 is not None and float(freq) > 0:
        expected_5min = float(freq) / (24 * 12)
        if expected_5min > 0:
            ratio = float(v5) / max(expected_5min, 0.01)
            freq_part = clamp01(math.tanh((ratio - 1.0) / 4.0))
    if features.get("amount_zscore") is None and features.get("user_event_frequency") is None:
        return 0.0
    if features.get("amount_zscore") is None:
        return freq_part
    if features.get("user_event_frequency") is None:
        return z_part
    return clamp01(0.7 * z_part + 0.3 * freq_part)


def score_event(
    event: dict[str, Any],
    features: dict[str, Any],
    org_config: dict[str, Any],
    baselines: Optional[Baselines] = None,
    model: Any = None,
) -> AnomalyResult:
    del baselines  # features already resolved by streaming
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    baseline_score = _baseline_score(features)
    contextual_score, reasons = _contextual_score(features)
    # attach live identifiers for reason templates
    enriched = dict(features)
    enriched.setdefault("device_id", event.get("device_id"))
    enriched.setdefault("ip_address", event.get("ip_address"))
    iforest_score = _iforest_score(model, enriched)
    if model is None:
        version = MODEL_VERSION_BASELINE_ONLY
        anomaly_score = clamp01(0.40 * baseline_score + 0.30 * contextual_score + 0.30 * 0.0)
    else:
        version = MODEL_VERSION
        anomaly_score = clamp01(0.40 * baseline_score + 0.30 * contextual_score + 0.30 * iforest_score)
        if iforest_score >= 0.7:
            reasons.append(
                Reason(
                    code="IFOREST_OUTLIER",
                    feature="iforest_score",
                    detail=f"Isolation Forest mapped score {iforest_score:.2f}",
                    contribution=0.30 * iforest_score,
                )
            )

    if features.get("thin_user_baseline"):
        reasons.append(
            Reason(
                code="THIN_USER_BASELINE_FALLBACK",
                feature="thin_user_baseline",
                detail="Fewer than 20 historical events; org/domain baselines used",
                contribution=0.0,
            )
        )

    # Distribute remaining contribution across rule reasons
    rule_weight = 0.30 * contextual_score
    rule_reasons = [r for r in reasons if r.code not in ("IFOREST_OUTLIER", "THIN_USER_BASELINE_FALLBACK")]
    if rule_reasons and rule_weight:
        share = rule_weight / len(rule_reasons)
        for r in rule_reasons:
            if r.contribution == 0.0:
                r.contribution = round(share, 4)
    z_reason = next((r for r in reasons if r.code == "AMOUNT_ZSCORE"), None)
    if z_reason:
        z_reason.contribution = round(max(z_reason.contribution, 0.40 * baseline_score * 0.7), 4)

    bands = org_config.get("severity_bands") or DEFAULT_SEVERITY_BANDS
    severity = severity_from_score(anomaly_score, bands)
    threshold = float(org_config.get("anomaly_score_threshold") or 0.75)
    return AnomalyResult(
        event_id=event["event_id"],
        organization_id=event["organization_id"],
        anomaly_score=anomaly_score,
        risk_score=anomaly_score,
        severity=severity,
        is_anomaly=anomaly_score >= threshold,
        reasons=reasons,
        model_version=version,
        processed_at=now,
        baseline_score=baseline_score,
        contextual_score=contextual_score,
        iforest_score=iforest_score,
    )
