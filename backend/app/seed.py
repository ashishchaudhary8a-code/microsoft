from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from backend.app.config import ORG_CONFIG_DIR
from backend.app.models import Organization, utcnow


def seed_organizations(session: Session, directory: str | Path | None = None) -> int:
    directory = Path(directory or ORG_CONFIG_DIR)
    if not directory.exists():
        return 0
    count = 0
    for path in sorted(directory.glob("*.json")):
        data: dict[str, Any] = json.loads(path.read_text())
        org_id = data["organization_id"]
        existing = session.get(Organization, org_id)
        fields = {
            "organization_name": data.get("organization_name") or org_id,
            "domain": data.get("domain") or "generic",
            "timezone": data.get("timezone") or "UTC",
            "currency": data.get("currency"),
            "config": data,
        }
        if existing is None:
            session.add(Organization(organization_id=org_id, created_at=utcnow(), **fields))
        else:
            for k, v in fields.items():
                setattr(existing, k, v)
        count += 1
    session.flush()
    return count
