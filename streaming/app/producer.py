from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any


def message_key(event: dict[str, Any]) -> bytes:
    org = event.get("organization_id") or ""
    user = event.get("user_id")
    raw = f"{org}:{user}" if user else org
    return raw.encode("utf-8")


def json_dumps(value: Any) -> bytes:
    return json.dumps(value, default=str, separators=(",", ":")).encode("utf-8")


def build_producer(bootstrap: str):
    from kafka import KafkaProducer

    return KafkaProducer(
        bootstrap_servers=bootstrap.split(","),
        key_serializer=lambda k: k if isinstance(k, bytes) else str(k).encode("utf-8"),
        value_serializer=json_dumps,
        linger_ms=5,
        retries=3,
        acks="all",
    )


def dlq_envelope(original_topic: str, code: str, message: str, payload: Any) -> dict[str, Any]:
    return {
        "original_topic": original_topic,
        "error_code": code,
        "error_message": message,
        "failed_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "payload": payload,
    }


def message_key(event: dict[str, Any]) -> bytes:
    org = event.get("organization_id") or ""
    user = event.get("user_id")
    raw = f"{org}:{user}" if user else org
    return raw.encode("utf-8")


def json_dumps(value: Any) -> bytes:
    return json.dumps(value, default=str, separators=(",", ":")).encode("utf-8")


def build_producer(bootstrap: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap.split(","),
        key_serializer=lambda k: k if isinstance(k, bytes) else str(k).encode("utf-8"),
        value_serializer=json_dumps,
        linger_ms=5,
        retries=3,
        acks="all",
    )


def dlq_envelope(original_topic: str, code: str, message: str, payload: Any) -> dict[str, Any]:
    return {
        "original_topic": original_topic,
        "error_code": code,
        "error_message": message,
        "failed_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "payload": payload,
    }
