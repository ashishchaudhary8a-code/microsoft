"""Hackathon-scale load stub (Locust).

If Locust is not installed, use the generator instead:
  docker compose run --rm generator python -m data.generators.generate --scenario normal --rate 50 --duration 300 --organization_id ORG_HOSPITAL_001

Targets (contract §23): 50 events/s for 5 minutes; API GET /api/dashboard p95 < 200ms.
"""

from __future__ import annotations

import os

from locust import HttpUser, between, task


ORG = os.environ.get("ORGANIZATION_ID", "ORG_HOSPITAL_001")


class DashboardUser(HttpUser):
    wait_time = between(0.05, 0.2)

    @task(5)
    def dashboard(self):
        self.client.get(f"/api/dashboard?organization_id={ORG}", name="/api/dashboard")

    @task(2)
    def events(self):
        self.client.get(f"/api/events?organization_id={ORG}&limit=50", name="/api/events")

    @task(1)
    def health(self):
        self.client.get("/api/health", name="/api/health")
