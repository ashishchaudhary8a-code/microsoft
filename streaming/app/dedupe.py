"""In-memory dedupe keyed by (organization_id, event_id) with TTL."""

from __future__ import annotations

import time
from collections import OrderedDict


class EventDeduper:
    def __init__(self, ttl_seconds: int = 3600, max_keys: int = 200_000):
        self.ttl = ttl_seconds
        self.max_keys = max_keys
        self._seen: OrderedDict[tuple[str, str], float] = OrderedDict()

    def seen(self, organization_id: str, event_id: str) -> bool:
        now = time.time()
        self._evict(now)
        key = (organization_id, event_id)
        if key in self._seen:
            return True
        self._seen[key] = now
        self._seen.move_to_end(key)
        if len(self._seen) > self.max_keys:
            self._seen.popitem(last=False)
        return False

    def _evict(self, now: float) -> None:
        cutoff = now - self.ttl
        while self._seen:
            key, ts = next(iter(self._seen.items()))
            if ts >= cutoff:
                break
            self._seen.popitem(last=False)
