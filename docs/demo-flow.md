# Streaming demo

1. `docker compose up --build`
2. Confirm health `ok` at http://localhost:8002/health
3. Generator emits normal events to `events.raw`
4. Watch metrics: `events_processed_total` on http://localhost:8001/metrics
5. `docker compose run --rm generator python -m data.generators.generate --scenario extreme --organization_id ORG_HOSPITAL_001`
6. Score ≥ 0.90 → `anomalies.detected` + `SINGLE_EXTREME_EVENT` on `alerts.generated`
7. Burst scenario → one `SUSPICIOUS_EVENT_BURST` when the window first crosses the threshold
8. `docker compose stop streaming` — lag grows, health 503
9. `docker compose start streaming` — consume from last committed offset

Backend persist and the React dashboard are other owners’ modules; this branch stops at Kafka topics + metrics.
