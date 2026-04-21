"""Loader for stagnation detection configuration from YAML."""

from __future__ import annotations

from pathlib import Path
from typing import List

import yaml

from pipewatch.stagnation import StagnationConfig


class StagnationConfigError(Exception):
    pass


def _parse_stagnation(raw: dict) -> StagnationConfig:
    if "pipeline" not in raw:
        raise StagnationConfigError("stagnation entry missing 'pipeline'")
    cfg = StagnationConfig(pipeline=raw["pipeline"])
    if "min_snapshots" in raw:
        cfg.min_snapshots = int(raw["min_snapshots"])
    if "window" in raw:
        cfg._window = int(raw["window"])
    if "tolerance" in raw:
        cfg.tolerance = float(raw["tolerance"])
    return cfg


def load_stagnation_configs(path: str = "pipewatch_stagnation.yml") -> List[StagnationConfig]:
    p = Path(path)
    if not p.exists():
        return []
    text = p.read_text()
    if not text.strip():
        return []
    data = yaml.safe_load(text)
    if not isinstance(data, dict):
        return []
    entries = data.get("stagnation", [])
    if not entries:
        return []
    return [_parse_stagnation(e) for e in entries]
