# Real-Time Anomaly Detection Platform

This branch delivers **Ashish’s streaming + data-engineering slice**: Redpanda ingest, validation, normalization, dedupe, feature-ready records, ML `score_event` call, alerts, DLQ, and Prometheus metrics.

Hospital / Hotel / Retail are demo orgs only. A new domain is org config + column mapping — no hardcoded domain engines.

## Quick start

```bash
cp .env.example .env
docker compose up --build
```

- Streaming health: http://localhost:8002/health
- Streaming metrics: http://localhost:8001/metrics
- Redpanda (host): `localhost:19092`

Inject scenarios:

```bash
docker compose run --rm generator python -m data.generators.generate --scenario extreme --organization_id ORG_HOSPITAL_001
docker compose run --rm generator python -m data.generators.generate --scenario burst --organization_id ORG_HOSPITAL_001
```

Invalid JSON / missing required fields land on `events.dlq`. Duplicates of `(organization_id, event_id)` are dropped (metric `events_deduped_total`), not DLQ’d.

## Streaming contract

Consumes `events.raw` → produces:

| Topic | Content |
| --- | --- |
| `events.processed` | Canonical event + features + processing timestamps |
| `anomalies.detected` | `score_event` result when `is_anomaly` |
| `alerts.generated` | `SINGLE_EXTREME_EVENT` (score ≥ 0.90) and `SUSPICIOUS_EVENT_BURST` (once per window) |
| `events.dlq` | Validation failures and ML exceptions |

Message key: `organization_id:user_id` if `user_id` is present, else `organization_id`.

ML exceptions do not crash the consumer: processed event is still produced; anomaly/alert skipped; `ml_failures_total` incremented.

## Tests

```bash
pip install -r streaming/requirements.txt -r ml/requirements.txt
PYTHONPATH=. pytest streaming/tests
```

## Manual steps (Ashish)

None for cloud accounts or API keys. Locally:

1. Docker Desktop / Compose available.
2. Optional: `cp .env.example .env` if you change the demo DB/broker settings (this branch does not start Postgres).
3. Isolation Forest artifacts are built by the `train` Compose service on first up (`python -m ml.train_all`).
