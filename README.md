# Real-Time Anomaly Detection Platform

Hospital / Hotel / Retail are demo orgs only. A new domain is org config + column mapping — no hardcoded domain engines.

## Quick start

```bash
cp .env.example .env
docker compose up --build
```

| Surface | URL |
| --- | --- |
| Dashboard | http://localhost:3000 |
| Backend API | http://localhost:8000/api/health |
| Streaming health | http://localhost:8002/health |
| Streaming metrics | http://localhost:8001/metrics |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3001 (admin / admin) |
| Redpanda (host) | localhost:19092 |

Default org: `ORG_HOSPITAL_001`. Switch orgs in the dashboard selector.

## What runs

Compose starts Redpanda (topics via `redpanda-init`), Postgres, Isolation Forest training (`train`), the streaming consumer, backend persist + REST + WebSocket, the React dashboard, the event generator, Prometheus, and Grafana.

Flow: generator → `events.raw` → streaming (`score_event`) → `events.processed` / `anomalies.detected` / `alerts.generated` → backend Postgres → `/api` + `/ws/live` → dashboard.

Invalid events go to `events.dlq`. ML exceptions do not crash the consumer.

## Demo inject

```bash
docker compose run --rm generator python -m data.generators.generate --scenario extreme --organization_id ORG_HOSPITAL_001
docker compose run --rm generator python -m data.generators.generate --scenario burst --organization_id ORG_HOSPITAL_001
docker compose run --rm generator python -m data.generators.generate --scenario invalid --organization_id ORG_HOSPITAL_001
```

Stop streaming to show health/freshness degrade: `docker compose stop streaming` then `docker compose start streaming`.

## Tests

```bash
pip install -r streaming/requirements.txt -r ml/requirements.txt -r backend/requirements.txt
PYTHONPATH=. pytest
```

Dashboard production image is built in Compose (`npm run build` inside `dashboard/Dockerfile`).

## Load test stub

Locust (optional):

```bash
pip install locust
locust -f monitoring/locustfile.py --host http://localhost:8000
```

Generator substitute (no Locust):

```bash
docker compose run --rm generator python -m data.generators.generate --scenario normal --rate 50 --duration 300 --organization_id ORG_HOSPITAL_001
```

## Manual steps

1. Docker Desktop / Compose available.
2. `cp .env.example .env` only if you change demo DB/broker passwords.
3. Isolation Forest artifacts are built by the `train` Compose service on first up.
4. No cloud accounts or API keys.
