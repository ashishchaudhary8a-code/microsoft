"""Train baselines + Isolation Forest for every org config with a history file."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from data.generators.generate import write_history  # noqa: E402
from ml.train import train_org  # noqa: E402


def main() -> int:
    artifact_dir = ROOT / "ml" / "artifacts"
    fixture_dir = ROOT / "data" / "fixtures"
    fixture_dir.mkdir(parents=True, exist_ok=True)
    for org_path in sorted((ROOT / "data" / "orgs").glob("*.json")):
        org = json.loads(org_path.read_text())
        org_id = org["organization_id"]
        hist = fixture_dir / f"{org_id}.history.jsonl"
        if not hist.exists():
            write_history(org_id, days=30, per_day=24, out=hist)
        events = [json.loads(line) for line in hist.read_text().splitlines() if line.strip()]
        train_org(events, org, artifact_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
