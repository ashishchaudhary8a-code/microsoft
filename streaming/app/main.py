"""Streaming consumer loop: events.raw → process → processed/anomaly/alert/DLQ."""

from __future__ import annotations

import logging
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable

from streaming.app.config import (
    CONSUMER_GROUP,
    KAFKA_BOOTSTRAP_SERVERS,
    METRICS_PORT,
    TOPIC_ALERTS,
    TOPIC_ANOMALIES,
    TOPIC_DLQ,
    TOPIC_PROCESSED,
    TOPIC_RAW,
)
from streaming.app.metrics import consumer_lag, service_health, start_metrics
from streaming.app.processor import StreamProcessor
from streaming.app.producer import build_producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("streaming.main")

HEALTH = {"status": "starting", "broker": "unknown"}


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        if self.path not in ("/health", "/"):
            self.send_response(404)
            self.end_headers()
            return
        body = (
            '{"status":"%s","streaming":"%s","broker":"%s"}'
            % (
                "ok" if HEALTH["status"] == "ok" else HEALTH["status"],
                HEALTH["status"],
                HEALTH["broker"],
            )
        ).encode()
        code = 200 if HEALTH["status"] == "ok" else 503
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args: Any) -> None:
        return


def wait_for_broker(bootstrap: str, timeout_s: int = 90) -> None:
    deadline = time.time() + timeout_s
    last = None
    while time.time() < deadline:
        try:
            producer = build_producer(bootstrap)
            producer.metrics()
            producer.close()
            return
        except Exception as exc:  # noqa: BLE001
            last = exc
            time.sleep(2)
    raise RuntimeError(f"broker not ready: {last}")


def update_lag(consumer: KafkaConsumer) -> None:
    try:
        parts = consumer.assignment()
        if not parts:
            return
        end = consumer.end_offsets(list(parts))
        for tp in parts:
            pos = consumer.position(tp)
            lag = max(int(end.get(tp, pos) - pos), 0)
            consumer_lag.labels(topic=tp.topic, partition=str(tp.partition)).set(lag)
    except Exception:
        log.debug("lag scrape failed", exc_info=True)


def run() -> None:
    start_metrics(METRICS_PORT)
    threading.Thread(
        target=lambda: ThreadingHTTPServer(("0.0.0.0", int(os.environ.get("STREAMING_HEALTH_PORT", "8002"))), HealthHandler).serve_forever(),
        daemon=True,
    ).start()

    bootstrap = KAFKA_BOOTSTRAP_SERVERS
    wait_for_broker(bootstrap)
    HEALTH["broker"] = "ok"
    topics = {
        "raw": TOPIC_RAW,
        "processed": TOPIC_PROCESSED,
        "anomalies": TOPIC_ANOMALIES,
        "alerts": TOPIC_ALERTS,
        "dlq": TOPIC_DLQ,
    }
    producer = build_producer(bootstrap)
    processor = StreamProcessor(producer, topics)

    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC_RAW,
                bootstrap_servers=bootstrap.split(","),
                group_id=CONSUMER_GROUP,
                enable_auto_commit=True,
                auto_offset_reset="earliest",
                value_deserializer=lambda v: v,
                key_deserializer=lambda v: v,
                consumer_timeout_ms=1000,
            )
            HEALTH["status"] = "ok"
            service_health.labels(component="streaming").set(1)
            log.info("consuming %s from %s", TOPIC_RAW, bootstrap)
            while True:
                records = consumer.poll(timeout_ms=500, max_records=100)
                if not records:
                    update_lag(consumer)
                    continue
                for _tp, batch in records.items():
                    for msg in batch:
                        processor.process_raw_json(msg.value)
                producer.flush()
                update_lag(consumer)
        except NoBrokersAvailable:
            HEALTH["status"] = "degraded"
            HEALTH["broker"] = "down"
            service_health.labels(component="streaming").set(0)
            log.warning("broker unavailable; retrying")
            time.sleep(2)
        except KafkaError:
            HEALTH["status"] = "degraded"
            service_health.labels(component="streaming").set(0)
            log.exception("kafka error; restarting consumer")
            time.sleep(2)


if __name__ == "__main__":
    run()
