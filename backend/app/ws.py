from __future__ import annotations

import asyncio
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from fastapi import WebSocket

from backend.app.freshness import snapshot as freshness_snapshot


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


class Hub:
    def __init__(self) -> None:
        self._clients: dict[str, set[WebSocket]] = defaultdict(set)
        self._lock = asyncio.Lock()

    async def connect(self, organization_id: str, ws: WebSocket) -> None:
        await ws.accept()
        async with self._lock:
            self._clients[organization_id].add(ws)

    async def disconnect(self, organization_id: str, ws: WebSocket) -> None:
        async with self._lock:
            self._clients[organization_id].discard(ws)

    async def emit(self, organization_id: str, type_: str, payload: dict[str, Any]) -> None:
        frame = {
            "type": type_,
            "organization_id": organization_id,
            "emitted_at": iso_now(),
            "payload": payload,
        }
        async with self._lock:
            targets = list(self._clients.get(organization_id, ()))
        dead: list[WebSocket] = []
        for ws in targets:
            try:
                await ws.send_json(frame)
            except Exception:
                dead.append(ws)
        if dead:
            async with self._lock:
                for ws in dead:
                    self._clients[organization_id].discard(ws)

    def client_count(self, organization_id: str | None = None) -> int:
        if organization_id:
            return len(self._clients.get(organization_id, ()))
        return sum(len(v) for v in self._clients.values())


hub = Hub()


async def emit_event(organization_id: str, payload: dict[str, Any]) -> None:
    await hub.emit(organization_id, "event", payload)


async def emit_anomaly(organization_id: str, payload: dict[str, Any]) -> None:
    await hub.emit(organization_id, "anomaly", payload)


async def emit_alert(organization_id: str, payload: dict[str, Any]) -> None:
    await hub.emit(organization_id, "alert", payload)


async def emit_dashboard(organization_id: str, payload: dict[str, Any]) -> None:
    await hub.emit(organization_id, "dashboard", payload)


async def emit_freshness(organization_id: str, sla: float = 5.0) -> None:
    await hub.emit(organization_id, "freshness", freshness_snapshot(organization_id, sla))


async def emit_health(organization_id: str, payload: dict[str, Any]) -> None:
    # health is cluster-wide but contract still scoped by subscription org
    await hub.emit(organization_id, "health", payload)
