# Demo flow (contract §30)

1. `cp .env.example .env` (optional) then `docker compose up --build`.
2. Open http://localhost:3000 — Live Events should fill from the hospital generator.
3. Overview shows events/min, totals, anomalies, critical count, average score.
4. Confirm **Data freshness: Xs** with GREEN while the pipeline is healthy.
5. `docker compose run --rm generator python -m data.generators.generate --scenario extreme --organization_id ORG_HOSPITAL_001`
6. The event scores high (often CRITICAL) with reasons on Anomaly Detail.
7. Dashboard updates over `/ws/live` without a full reload.
8. Anomaly Center shows `SINGLE_EXTREME_EVENT` with reasons.
9. `docker compose run --rm generator python -m data.generators.generate --scenario burst --organization_id ORG_HOSPITAL_001`
10. Burst alert `SUSPICIOUS_EVENT_BURST` appears once the window threshold is crossed.
11. `docker compose stop streaming` (or `backend`).
12. System Health degrades; freshness moves WARNING/CRITICAL.
13. `docker compose start streaming`.
14. Processing resumes from the last committed offset. Invalid JSON still goes to `events.dlq` (`--scenario invalid`).
