# Streaming (Ashish)

Redpanda consumer for `events.raw`. Validate → normalize → dedupe → feature prep → `ml.score_event` → produce `events.processed`, `anomalies.detected`, `alerts.generated`. Invalids and ML errors go to `events.dlq` without crashing the loop.

## Run in Compose

```bash
docker compose up --build redpanda redpanda-init train streaming
```

Health: `http://localhost:8002/health`  
Metrics: `http://localhost:8001/metrics`

## Tests

```bash
PYTHONPATH=. pytest streaming/tests
```

## Manual (host, not Compose)

1. Redpanda reachable at `KAFKA_BOOTSTRAP_SERVERS` (Compose uses `redpanda:9092`, host uses `localhost:19092`).
2. Topics created (`redpanda-init` does this).
3. Org JSON in `data/orgs/` and ML artifacts in `ml/artifacts/` (`train` service or `python -m ml.train_all`).
4. No cloud API keys. Local demo password lives only in `.env.example`.
