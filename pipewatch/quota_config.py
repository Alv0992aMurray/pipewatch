"""Load quota configurations from YAML."""
from __future__ import annotations

from pathlib import Path
from typing import List

import yaml

from pipewatch.quota import QuotaConfig


class QuotaConfigError(Exception):
    pass


def _parse_quota(raw: dict) -> QuotaConfig:
    if "pipeline" not in raw:
        raise QuotaConfigError("quota entry missing 'pipeline'")
    if "max_alerts" not in raw:
        raise QuotaConfigError("quota entry missing 'max_alerts'")
    return QuotaConfig(
        pipeline=raw["pipeline"],
        max_alerts=int(raw["max_alerts"]),
        window_minutes=int(raw.get("window_minutes", 60)),
    )


def load_quota_configs(path: str | Path = "pipewatch_quotas.yml") -> List[QuotaConfig]:
    p = Path(path)
    if not p.exists():
        return []
    text = p.read_text()
    if not text.strip():
        return []
    data = yaml.safe_load(text)
    if not data or "quotas" not in data:
        return []
    return [_parse_quota(entry) for entry in data["quotas"]]
