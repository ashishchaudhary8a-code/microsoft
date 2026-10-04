from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


DATABASE_URL = _env(
    "DATABASE_URL",
    "postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}".format(
        user=_env("POSTGRES_USER", "anomaly"),
        password=_env("POSTGRES_PASSWORD", "anomaly_demo"),
        host=_env("POSTGRES_HOST", "localhost"),
        port=_env("POSTGRES_PORT", "5432"),
        db=_env("POSTGRES_DB", "anomaly"),
    ),
)

KAFKA_BOOTSTRAP_SERVERS = _env("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
BACKEND_CONSUMER_GROUP = _env("BACKEND_CONSUMER_GROUP", "backend-persist")
ORG_CONFIG_DIR = _env("ORG_CONFIG_DIR", str(ROOT / "data" / "orgs"))
STREAMING_HEALTH_URL = _env("STREAMING_HEALTH_URL", "http://localhost:8002/health")
METRICS_PORT = int(_env("BACKEND_METRICS_PORT", "0"))  # 0 = expose /metrics on the API process
SEED_ORGS = _env("SEED_ORGS", "1") == "1"
CONSUME_TOPICS = _env("BACKEND_CONSUME", "1") == "1"

TOPIC_PROCESSED = "events.processed"
TOPIC_ANOMALIES = "anomalies.detected"
TOPIC_ALERTS = "alerts.generated"
