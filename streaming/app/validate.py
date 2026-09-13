"""Validate canonical events. Invalid → DLQ, never persist as valid."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

REQUIRED_FIELDS = ("event_id", "organization_id", "domain", "event_type", "timestamp")


class ValidationError(Exception):
    def __init__(self, message: str, code: str = "VALIDATION_ERROR"):
        super().__init__(message)
        self.code = code
        self.message = message


def parse_utc_timestamp(value: Any) -> datetime:
    if value is None or value == "":
        raise ValidationError("missing timestamp")
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        ts = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValidationError("invalid timestamp") from exc
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def validate_event(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValidationError("payload is not a JSON object")
    missing = [f for f in REQUIRED_FIELDS if payload.get(f) in (None, "")]
    if missing:
        raise ValidationError(f"missing required field(s): {', '.join(missing)}")
    for field in REQUIRED_FIELDS:
        if not isinstance(payload[field], str):
            raise ValidationError(f"{field} must be a string")
    parse_utc_timestamp(payload["timestamp"])
    if "amount" in payload and payload["amount"] is not None:
        if isinstance(payload["amount"], bool) or not isinstance(payload["amount"], (int, float)):
            raise ValidationError("amount must be a number")
    return payload
