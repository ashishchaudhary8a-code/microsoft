from __future__ import annotations

from collections import deque

from prometheus_client import Counter, Gauge, Histogram, start_http_server

events_received_total = Counter("events_received_total", "Events read from events.raw")
events_processed_total = Counter("events_processed_total", "Events produced to events.processed")
events_failed_total = Counter("events_failed_total", "Events failed validation or processing")
events_deduped_total = Counter("events_deduped_total", "Duplicate events dropped")
events_dropped_total = Counter("events_dropped_total", "Events dropped from overflow buffers")
ml_failures_total = Counter("ml_failures_total", "score_event exceptions")
anomalies_total = Counter("anomalies_total", "Events with is_anomaly true")
critical_anomalies_total = Counter("critical_anomalies_total", "Events with CRITICAL severity")
alerts_total = Counter("alerts_total", "Alerts produced", ["alert_type"])
events_per_second = Gauge("events_per_second", "Processed events per second (1s window)")
processing_latency_seconds = Histogram(
    "processing_latency_seconds",
    "Receive to processed latency",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5),
)
processing_latency_p95_seconds = Gauge(
    "processing_latency_p95_seconds",
    "Approximate P95 processing latency from recent samples",
)
consumer_lag = Gauge("consumer_lag", "Approx consumer lag (end offset - position)", ["topic", "partition"])
service_health = Gauge("service_health", "1 if streaming loop is healthy", ["component"])

_recent_latencies: deque[float] = deque(maxlen=500)


def record_processing_latency(seconds: float) -> None:
    value = max(float(seconds), 0.0)
    processing_latency_seconds.observe(value)
    _recent_latencies.append(value)
    processing_latency_p95_seconds.set(percentile(list(_recent_latencies), 0.95))


def percentile(samples: list[float], q: float) -> float:
    if not samples:
        return 0.0
    ordered = sorted(samples)
    idx = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * q))))
    return float(ordered[idx])


def start_metrics(port: int) -> None:
    start_http_server(port)
    service_health.labels(component="streaming").set(1)
    service_health.labels(component="ml").set(1)
