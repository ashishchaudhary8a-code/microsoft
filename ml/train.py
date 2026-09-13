"""Batch training: historical events → baselines + Isolation Forest artifacts."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from ml.scoring import Baselines, feature_vector, fit_baselines, fit_model

try:
    import joblib
except ImportError:  # pragma: no cover
    joblib = None


def _read_jsonl(path: Path) -> list[dict]:
    rows = []
    with path.open() as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def _load_org_config(path: Path) -> dict:
    return json.loads(path.read_text())


def train_org(
    events: list[dict],
    org_config: dict,
    artifact_dir: Path,
) -> None:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    org_id = org_config["organization_id"]
    baselines = fit_baselines(events, org_config)
    (artifact_dir / f"{org_id}.baselines.json").write_text(json.dumps(baselines.to_json(), indent=2))

    from ml.scoring import score_event  # used only to keep import graph stable

    del score_event
    matrix = []
    # Feature rows for IF: reconstruct simple historical features from baselines
    users = baselines.users
    org = baselines.org
    for ev in events:
        uid = ev.get("user_id")
        ub = users.get(uid, org) if uid else org
        amount = ev.get("amount")
        z = None
        if amount is not None and ub.amount_avg is not None and ub.amount_std:
            z = (float(amount) - ub.amount_avg) / (ub.amount_std or 1.0)
        loc = ev.get("location")
        features = {
            "amount": amount,
            "user_amount_avg": ub.amount_avg,
            "user_amount_std": ub.amount_std,
            "amount_zscore": z,
            "events_last_5min": 1,
            "events_last_hour": 1,
            "distance_from_usual": 0 if loc and loc == ub.usual_location else (1 if loc else None),
            "new_device": False,
            "new_ip": False,
            "unusual_time": False,
            "hour_of_day": 12,
            "time_since_previous_event_seconds": 3600,
            "user_event_frequency": ub.events_per_day,
        }
        matrix.append(feature_vector(features))

    model = fit_model(matrix, org_config)
    if model is not None and joblib is not None:
        joblib.dump(model, artifact_dir / f"{org_id}.iforest.joblib")
    manifest = {
        "organization_id": org_id,
        "n_events": len(events),
        "n_users": len(baselines.users),
        "has_iforest": model is not None,
    }
    (artifact_dir / f"{org_id}.manifest.json").write_text(json.dumps(manifest, indent=2))
    print(f"trained {org_id}: events={len(events)} users={len(baselines.users)} iforest={model is not None}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Fit baselines and Isolation Forest per org")
    parser.add_argument("--history", required=True, help="JSONL historical events")
    parser.add_argument("--org-config", required=True, help="Org config JSON")
    parser.add_argument("--artifact-dir", default="ml/artifacts")
    args = parser.parse_args(argv)
    events = _read_jsonl(Path(args.history))
    org_config = _load_org_config(Path(args.org_config))
    train_org(events, org_config, Path(args.artifact_dir))
    return 0


if __name__ == "__main__":
    sys.exit(main())
