"""Streaming process configuration."""

from __future__ import annotations

import os


def _int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
CONSUMER_GROUP = os.environ.get("STREAMING_CONSUMER_GROUP", "streaming-0")
CONSUMER_ID = os.environ.get("STREAMING_CONSUMER_ID", "streaming-0")
TOPIC_RAW = "events.raw"
TOPIC_PROCESSED = "events.processed"
TOPIC_ANOMALIES = "anomalies.detected"
TOPIC_ALERTS = "alerts.generated"
TOPIC_DLQ = "events.dlq"
METRICS_PORT = _int("STREAMING_METRICS_PORT", 8001)
ML_ARTIFACT_DIR = os.environ.get("ML_ARTIFACT_DIR", "ml/artifacts")
ORG_CONFIG_DIR = os.environ.get("ORG_CONFIG_DIR", "data/orgs")
DEDUPE_TTL_SECONDS = _int("DEDUPE_TTL_SECONDS", 3600)
PARTITIONS = 3
