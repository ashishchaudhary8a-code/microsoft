"""Load baselines and Isolation Forest artifacts from disk."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional

from ml.scoring import Baselines

try:
    import joblib
except ImportError:  # pragma: no cover
    joblib = None


class ArtifactStore:
    def __init__(self, artifact_dir: str | Path):
        self.dir = Path(artifact_dir)
        self._baselines: dict[str, Baselines] = {}
        self._models: dict[str, Any] = {}

    def load(self) -> None:
        self.dir.mkdir(parents=True, exist_ok=True)
        for path in self.dir.glob("*.baselines.json"):
            data = json.loads(path.read_text())
            b = Baselines.from_json(data)
            self._baselines[b.organization_id] = b
        if joblib:
            for path in self.dir.glob("*.iforest.joblib"):
                org_id = path.name.replace(".iforest.joblib", "")
                self._models[org_id] = joblib.load(path)

    def baselines_for(self, organization_id: str) -> Optional[Baselines]:
        return self._baselines.get(organization_id)

    def model_for(self, organization_id: str) -> Any:
        return self._models.get(organization_id)

    def put_baselines(self, baselines: Baselines) -> None:
        self._baselines[baselines.organization_id] = baselines
