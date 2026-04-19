"""Load lifecycle configuration from YAML."""
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import List
import yaml


class LifecycleConfigError(Exception):
    pass


@dataclass
class LifecycleConfig:
    pipeline: str
    window: int = 5


def _parse_config(raw: dict) -> LifecycleConfig:
    if "pipeline" not in raw:
        raise LifecycleConfigError("lifecycle entry missing 'pipeline'")
    return LifecycleConfig(
        pipeline=raw["pipeline"],
        window=int(raw.get("window", 5)),
    )


def load_lifecycle_configs(path: str = "pipewatch_lifecycle.yml") -> List[LifecycleConfig]:
    p = Path(path)
    if not p.exists():
        return []
    data = yaml.safe_load(p.read_text()) or {}
    entries = data.get("lifecycles", [])
    if not entries:
        return []
    return [_parse_config(e) for e in entries]
