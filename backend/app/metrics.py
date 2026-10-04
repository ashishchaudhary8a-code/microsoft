from __future__ import annotations

from prometheus_client import CollectorRegistry, CONTENT_TYPE_LATEST, Gauge, generate_latest

REGISTRY = CollectorRegistry()

freshness_seconds = Gauge(
    "freshness_seconds",
    "Latest event-to-dashboard seconds",
    ["organization_id"],
    registry=REGISTRY,
)
freshness_p95_seconds = Gauge(
    "freshness_p95_seconds",
    "P95 event-to-dashboard seconds",
    ["organization_id"],
    registry=REGISTRY,
)
service_health = Gauge("service_health", "1 if component is healthy", ["component"], registry=REGISTRY)
backend_events_ingested_total = Gauge(
    "backend_events_ingested_total",
    "Events persisted by backend",
    registry=REGISTRY,
)


def metrics_response() -> tuple[bytes, str]:
    return generate_latest(REGISTRY), CONTENT_TYPE_LATEST


def set_component_health(component: str, ok: bool) -> None:
    service_health.labels(component=component).set(1 if ok else 0)
