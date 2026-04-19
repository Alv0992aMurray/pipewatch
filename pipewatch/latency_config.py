"""Load latency detection configs from YAML."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List

import yaml


class LatencyConfigError(Exception):
    pass


@dataclass
class LatencyConfig:
    pipeline: str
    threshold_seconds: float = 300.0


def _parse_config(raw: dict) -> LatencyConfig:
    if "pipeline" not in raw:
        raise LatencyConfigError("latency config missing 'pipeline'")
    return LatencyConfig(
        pipeline=raw["pipeline"],
        threshold_seconds=float(raw.get("threshold_seconds", 300.0)),
    )


def load_latency_configs(path: str = "pipewatch_latency.yml") -> List[LatencyConfig]:
    p = Path(path)
    if not p.exists():
        return []
    data = yaml.safe_load(p.read_text()) or {}
    raw_list = data.get("latency", [])
    if not raw_list:
        return []
    return [_parse_config(r) for r in raw_list]
