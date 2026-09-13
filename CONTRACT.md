# Real-Time Anomaly Detection Platform — contract (working copy)

Authoritative PDF: `CONTRACT.pdf`. This file restates frozen interfaces for implementers.

## Product

Domain-agnostic platform: org config + historical events + live events ? baselines ? live scores ? (A) single extreme event and (B) suspicious burst ? dashboard with scores, reasons, alerts, freshness.

## Canonical event

Required: `event_id`, `organization_id`, `domain`, `event_type`, `timestamp` (UTC ISO 8601).  
Optional: `user_id`, `username`, `location`, `device_id`, `ip_address`, `action`, `status`, `amount`, `currency`, `payload`, `metadata`.

## Topics

`events.raw` ? streaming ? `events.processed`, `anomalies.detected`, `alerts.generated`; invalids ? `events.dlq`.  
Key: `organization_id:user_id` or `organization_id`.

## ML

`score_event(event, features, org_config, baselines) -> AnomalyResult`  
`fit_baselines(historical_events, org_config) -> Baselines`  
`fit_model(feature_matrix, org_config) -> ModelArtifact`

Blend (proposed): `0.40 * baseline + 0.30 * contextual + 0.30 * iforest`.  
`SINGLE_EXTREME_EVENT` when score ? 0.90.  
Burst: ? `suspicious_event_count` events with score ? `suspicious_score_threshold` in `suspicious_event_window_seconds` for the same `user_id`.

## API

Base `/api`. Org-scoped GETs require `organization_id`.  
Endpoints: health, organizations, dashboard, events, anomalies, anomaly detail, alerts, pipeline freshness.  
WS: `/ws/live?organization_id=` types `event`, `anomaly`, `alert`, `dashboard`, `freshness`, `health`.

## Dashboard pages

Overview, Live Events, Anomaly Center, Anomaly Detail, System Health. No others.

## Stack

Python, FastAPI, PostgreSQL, Redpanda, React, sklearn, Prometheus, Grafana, Docker Compose.
