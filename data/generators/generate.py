"""Synthetic event generator. Emits canonical events to events.raw (or stdout)."""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
import uuid
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]

ROOT = Path(__file__).resolve().parents[2]


def load_org(org_id: str) -> dict[str, Any]:
    path = ROOT / "data" / "orgs" / f"{org_id}.json"
    return json.loads(path.read_text())


def iso_z(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def message_key(event: dict[str, Any]) -> bytes:
    org = event["organization_id"]
    user = event.get("user_id")
    return (f"{org}:{user}" if user else org).encode()


USERS = {
    "ORG_HOSPITAL_001": [
        ("USR_204", "employee_204", "Pune", "DEVICE_12", "10.0.0.14"),
        ("USR_118", "employee_118", "Mumbai", "DEVICE_4", "10.0.0.18"),
        ("USR_055", "employee_055", "Pune", "DEVICE_9", "10.0.0.22"),
    ],
    "ORG_HOTEL_001": [
        ("USR_H01", "clerk_01", "Goa", "POS_1", "10.1.0.10"),
        ("USR_H02", "clerk_02", "Jaipur", "POS_2", "10.1.0.11"),
    ],
    "ORG_RETAIL_001": [
        ("USR_R01", "assoc_01", "Boston", "REG_1", "10.2.0.10"),
        ("USR_R02", "assoc_02", "Chicago", "REG_2", "10.2.0.11"),
    ],
}

NORMAL_AMOUNT = {
    "ORG_HOSPITAL_001": (3500, 900),
    "ORG_HOTEL_001": (8000, 1500),
    "ORG_RETAIL_001": (45, 12),
}

ACTIONS = ["create", "update", "complete", "payment", "checkin"]


def make_event(org: dict[str, Any], user: tuple[str, str, str, str, str], ts: datetime, amount: float, **overrides: Any) -> dict[str, Any]:
    event = {
        "event_id": "EVT_" + uuid.uuid4().hex[:10].upper(),
        "organization_id": org["organization_id"],
        "domain": org["domain"],
        "event_type": "event",
        "timestamp": iso_z(ts),
        "user_id": user[0],
        "username": user[1],
        "location": user[2],
        "device_id": user[3],
        "ip_address": user[4],
        "action": random.choice(ACTIONS),
        "status": "completed",
        "amount": round(amount, 2),
        "currency": org.get("currency"),
        "payload": {},
        "metadata": {"scenario": "normal"},
    }
    event.update(overrides)
    return event


def normal_event(org: dict[str, Any], rng: random.Random, ts: datetime | None = None) -> dict[str, Any]:
    users = USERS[org["organization_id"]]
    user = rng.choice(users)
    mean, std = NORMAL_AMOUNT[org["organization_id"]]
    amount = max(1.0, rng.gauss(mean, std))
    hour = rng.choice([9, 10, 11, 12, 13, 14, 15, 16, 17])
    now = ts or datetime.now(timezone.utc).replace(hour=hour, minute=rng.randint(0, 59), second=rng.randint(0, 59), microsecond=0)
    return make_event(org, user, now, amount)


def extreme_event(org: dict[str, Any], rng: random.Random) -> dict[str, Any]:
    user = USERS[org["organization_id"]][0]
    mean, _ = NORMAL_AMOUNT[org["organization_id"]]
    amount = mean * 18
    ts = datetime.now(timezone.utc)
    return make_event(
        org,
        user,
        ts,
        amount,
        location="Unknown-City",
        device_id="DEVICE_991",
        ip_address="192.168.1.20",
        action="payment",
        metadata={"scenario": "extreme"},
        timestamp=iso_z(ts.replace(hour=2, minute=31, second=0, microsecond=0)) if ts.hour > 3 else iso_z(ts),
    )


def burst_events(org: dict[str, Any], rng: random.Random, count: int = 4) -> list[dict[str, Any]]:
    user = USERS[org["organization_id"]][0]
    mean, std = NORMAL_AMOUNT[org["organization_id"]]
    base = datetime.now(timezone.utc)
    events = []
    for i in range(count):
        amount = mean * 4 + rng.random() * std
        ts = base + timedelta(seconds=i * 8)
        events.append(
            make_event(
                org,
                user,
                ts,
                amount,
                device_id="DEVICE_NEW_" + str(i),
                ip_address=f"203.0.113.{10 + i}",
                metadata={"scenario": "burst"},
            )
        )
    return events


class BufferedProducer:
    def __init__(self, bootstrap: str, max_buffer: int = 10_000):
        from kafka import KafkaProducer
        from kafka.errors import NoBrokersAvailable

        self.KafkaProducer = KafkaProducer
        self.NoBrokersAvailable = NoBrokersAvailable
        self.bootstrap = bootstrap
        self.buffer: deque = deque()
        self.max_buffer = max_buffer
        self.dropped = 0
        self.producer = None
        self._connect()

    def _connect(self) -> None:
        try:
            self.producer = self.KafkaProducer(
                bootstrap_servers=self.bootstrap.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k if isinstance(k, bytes) else str(k).encode(),
                retries=3,
                linger_ms=5,
            )
        except self.NoBrokersAvailable:
            self.producer = None

    def send(self, event: dict[str, Any]) -> None:
        if self.producer is None:
            self._connect()
        payload = event
        try:
            if self.producer is None:
                raise self.NoBrokersAvailable()
            self.producer.send("events.raw", key=message_key(event), value=payload)
            while self.buffer:
                queued = self.buffer.popleft()
                self.producer.send("events.raw", key=message_key(queued), value=queued)
        except Exception:
            self.buffer.append(payload)
            if len(self.buffer) > self.max_buffer:
                self.buffer.popleft()
                self.dropped += 1
            self.producer = None


def write_history(org_id: str, days: int, per_day: int, out: Path, seed: int = 42) -> None:
    rng = random.Random(seed)
    org = load_org(org_id)
    start = datetime.now(timezone.utc) - timedelta(days=days)
    out.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with out.open("w") as fh:
        for day in range(days):
            for i in range(per_day):
                ts = start + timedelta(days=day, minutes=i * max(1, int(24 * 60 / per_day)))
                if ts.hour < 8 or ts.hour > 18:
                    ts = ts.replace(hour=10)
                ev = normal_event(org, rng, ts)
                fh.write(json.dumps(ev) + "\n")
                n += 1
    print(f"history {org_id}: {n} rows → {out}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--organization_id", default="ORG_HOSPITAL_001")
    parser.add_argument("--scenario", choices=("normal", "extreme", "burst", "history"), default="normal")
    parser.add_argument("--rate", type=float, default=5.0, help="events/sec for normal")
    parser.add_argument("--duration", type=int, default=0, help="seconds; 0 = forever")
    parser.add_argument("--stdout", action="store_true")
    parser.add_argument("--history-out", default="")
    parser.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"))
    args = parser.parse_args(argv)
    org = load_org(args.organization_id)
    rng = random.Random()

    if args.scenario == "history":
        write_history(args.organization_id, 30, 20, Path(args.history_out or f"data/fixtures/{args.organization_id}.history.jsonl"))
        return 0

    def emit(events: list[dict[str, Any]]) -> None:
        if args.stdout:
            for e in events:
                print(json.dumps(e))
            return
        prod = BufferedProducer(args.bootstrap)
        for e in events:
            prod.send(e)
        if prod.producer:
            prod.producer.flush()

    if args.scenario == "extreme":
        emit([extreme_event(org, rng)])
        return 0
    if args.scenario == "burst":
        emit(burst_events(org, rng, 4))
        return 0

    prod = None if args.stdout else BufferedProducer(args.bootstrap)
    start = time.time()
    while True:
        ev = normal_event(org, rng, datetime.now(timezone.utc))
        if args.stdout:
            print(json.dumps(ev), flush=True)
        else:
            assert prod is not None
            prod.send(ev)
        if args.rate > 0:
            time.sleep(1.0 / args.rate)
        if args.duration and (time.time() - start) >= args.duration:
            break
    if prod and prod.producer:
        prod.producer.flush()
    return 0


if __name__ == "__main__":
    sys.exit(main())
