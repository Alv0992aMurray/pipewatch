"""Load cooldown configurations from YAML."""

from __future__ import annotations

from pathlib import Path
from typing import List

import yaml

from pipewatch.cooldown import CooldownConfig


class CooldownConfigError(Exception):
    pass


def _parse_cooldown(raw: dict) -> CooldownConfig:
    if "pipeline" not in raw:
        raise CooldownConfigError("Cooldown entry missing required field: pipeline")
    if "seconds" not in raw:
        raise CooldownConfigError("Cooldown entry missing required field: seconds")
    seconds = int(raw["seconds"])
    if seconds <= 0:
        raise CooldownConfigError(f"Cooldown seconds must be positive, got {seconds}")
    return CooldownConfig(pipeline=str(raw["pipeline"]), seconds=seconds)


def load_cooldown_configs(path: str = "pipewatch_cooldowns.yml") -> List[CooldownConfig]:
    p = Path(path)
    if not p.exists():
        return []
    with p.open() as fh:
        data = yaml.safe_load(fh)
    if not data:
        return []
    entries = data.get("cooldowns", [])
    if not entries:
        return []
    return [_parse_cooldown(entry) for entry in entries]
