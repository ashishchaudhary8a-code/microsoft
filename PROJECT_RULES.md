# Project rules

Work from `CONTRACT.md` / `CONTRACT.pdf`. Do not invent extra product surface area.

## Ownership

| Owner | Module | Does not own |
| --- | --- | --- |
| Kavisha | `backend/` | Broker topology, sklearn, React pages |
| Ashish | `streaming/` | Whole backend, ML model design/training |
| Divay | `ml/` | HTTP API, UI, broker ops |
| Avni | `dashboard/` | FastAPI, streaming |
| Akshara | `data/` | Production consumer, ML internals |
| Keshav | Compose, Prometheus, Grafana, load test | Product pages, model math |

## Hard rules

- Unit of work is an **EVENT**. Do not special-case “transaction”.
- Hospital and Hotel are demo domains only. New domains onboard via org config + column mapping.
- All queries filter `organization_id` except global health.
- No DL / LLMs. No Kubernetes. No extra databases.
- Invalid events go to DLQ; never persist as valid.
- ML exceptions must not crash the streaming consumer.
- Folders at repo root match section 26 of the contract.
