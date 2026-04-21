"""Load smoothing configurations from YAML."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List

import yaml

_DEFAULT_ALPHA = 0.3
_DEFAULT_METRIC = "success_rate"
_DEFAULT_MIN_POINTS = 2


class SmoothingConfigError(Exception):
    """Raised when a smoothing config entry is invalid."""


@dataclass
class SmoothingConfig:
    pipeline: str
    metric: str = _DEFAULT_METRIC
    alpha: float = _DEFAULT_ALPHA
    min_points: int = _DEFAULT_MIN_POINTS


def _parse_config(raw: dict) -> SmoothingConfig:
    if "pipeline" not in raw:
        raise SmoothingConfigError("smoothing entry missing required 'pipeline' key")
    alpha = float(raw.get("alpha", _DEFAULT_ALPHA))
    if not (0.0 < alpha <= 1.0):
        raise SmoothingConfigError(
            f"alpha must be in (0, 1], got {alpha}"
        )
    return SmoothingConfig(
        pipeline=raw["pipeline"],
        metric=raw.get("metric", _DEFAULT_METRIC),
        alpha=alpha,
        min_points=int(raw.get("min_points", _DEFAULT_MIN_POINTS)),
    )


def load_smoothing_configs(path: str = "pipewatch_smoothing.yml") -> List[SmoothingConfig]:
    """Load smoothing configs from *path*; return empty list if file is absent."""
    p = Path(path)
    if not p.exists():
        return []
    data = yaml.safe_load(p.read_text()) or {}
    entries = data.get("smoothing", [])
    if not entries:
        return []
    return [_parse_config(e) for e in entries]
