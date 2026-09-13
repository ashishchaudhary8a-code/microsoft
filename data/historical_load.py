"""Map source rows into the canonical event schema using data/mappings/<domain>.json."""

from __future__ import annotations

import argparse
import csv
import json
import sys
import uuid
from pathlib import Path
from typing import Any


def load_mapping(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def map_row(row: dict[str, Any], mapping: dict[str, Any], index: int) -> dict[str, Any] | None:
    column_map: dict[str, str] = mapping.get("column_map") or {}
    defaults: dict[str, Any] = dict(mapping.get("defaults") or {})
    event: dict[str, Any] = dict(defaults)
    event["organization_id"] = mapping.get("organization_id") or event.get("organization_id")
    event["domain"] = mapping.get("domain") or event.get("domain")
    for src, dest in column_map.items():
        if src in row and row[src] not in (None, ""):
            event[dest] = row[src]
    if "event_id" not in event or not event["event_id"]:
        event["event_id"] = f"SYN_{uuid.uuid4().hex[:12]}_{index}"
    required = ("event_id", "organization_id", "domain", "event_type", "timestamp")
    if any(not event.get(f) for f in required):
        return None
    if "amount" in event:
        try:
            event["amount"] = float(event["amount"])
        except (TypeError, ValueError):
            return None
    event.setdefault("payload", {})
    event.setdefault("metadata", {})
    return event


def load_tabular(path: Path) -> list[dict[str, Any]]:
    if path.suffix.lower() == ".json":
        data = json.loads(path.read_text())
        return data if isinstance(data, list) else data.get("rows") or []
    if path.suffix.lower() == ".jsonl":
        rows = []
        for line in path.read_text().splitlines():
            if line.strip():
                rows.append(json.loads(line))
        return rows
    with path.open(newline="") as fh:
        return list(csv.DictReader(fh))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Normalize historical files into canonical JSONL")
    parser.add_argument("--input", required=True)
    parser.add_argument("--mapping", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    mapping = load_mapping(Path(args.mapping))
    rows = load_tabular(Path(args.input))
    kept = 0
    skipped = 0
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w") as fh:
        for i, row in enumerate(rows):
            event = map_row(row, mapping, i)
            if event is None:
                skipped += 1
                continue
            fh.write(json.dumps(event) + "\n")
            kept += 1
    print(f"wrote {kept} events, skipped {skipped} invalid rows → {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
