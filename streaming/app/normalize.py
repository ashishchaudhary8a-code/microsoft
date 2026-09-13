"""Normalize types, UTC timestamps, default empty objects. Unknown fields ignored."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from streaming.app.validate import parse_utc_timestamp

KNOWN_FIELDS = {
    "event_id",
    "organization_id",
    "domain",
    "event_type",
    "timestamp",
    "user_id",
    "username",
    "location",
    "device_id",
    "ip_address",
    "action",
    "status",
    "amount",
    "currency",
    "payload",
    "metadata",
}


def to_iso_z(ts: datetime) -> str:
    utc = ts.astimezone(timezone.utc)
    return utc.strftime("%Y-%m-%dT%H:%M:%SZ")


def normalize_event(payload: dict[str, Any]) -> dict[str, Any]:
    ts = parse_utc_timestamp(payload["timestamp"])
    amount = payload.get("amount")
    if amount is not None:
        amount = float(amount)
    payload_obj = payload.get("payload")
    metadata_obj = payload.get("metadata")
    if not isinstance(payload_obj, dict):
        payload_obj = {}
    if not isinstance(metadata_obj, dict):
        metadata_obj = {}

    def opt_str(key: str) -> str | None:
        val = payload.get(key)
        if val is None or val == "":
            return None
        return str(val)

    return {
        "event_id": str(payload["event_id"]),
        "organization_id": str(payload["organization_id"]),
        "domain": str(payload["domain"]),
        "event_type": str(payload["event_type"]),
        "timestamp": to_iso_z(ts),
        "user_id": opt_str("user_id"),
        "username": opt_str("username"),
        "location": opt_str("location"),
        "device_id": opt_str("device_id"),
        "ip_address": opt_str("ip_address"),
        "action": opt_str("action"),
        "status": opt_str("status"),
        "amount": amount,
        "currency": opt_str("currency"),
        "payload": payload_obj,
        "metadata": metadata_obj,
    }
