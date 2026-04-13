"""Load per-pipeline forecast configuration from YAML."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List

import yaml


class ForecastConfigError(Exception):
    pass


@dataclass
class ForecastConfig:
    pipeline: str
    horizon: int = 1
    min_points: int = 3


def _parse_forecast(raw: dict, index: int) -> ForecastConfig:
    if "pipeline" not in raw:
        raise ForecastConfigError(
            f"Forecast entry #{index} missing required field 'pipeline'"
        )
    pipeline = str(raw["pipeline"])
    horizon = int(raw.get("horizon", 1))
    min_points = int(raw.get("min_points", 3))
    if horizon < 1:
        raise ForecastConfigError(
            f"Forecast entry for '{pipeline}': horizon must be >= 1"
        )
    if min_points < 2:
        raise ForecastConfigError(
            f"Forecast entry for '{pipeline}': min_points must be >= 2"
        )
    return ForecastConfig(pipeline=pipeline, horizon=horizon, min_points=min_points)


def load_forecast_configs(path: str | Path = "pipewatch_forecasts.yml") -> List[ForecastConfig]:
    p = Path(path)
    if not p.exists():
        raise ForecastConfigError(f"Forecast config file not found: {p}")
    with p.open() as fh:
        data = yaml.safe_load(fh)
    if not data:
        return []
    entries = data.get("forecasts", [])
    if not isinstance(entries, list):
        raise ForecastConfigError("'forecasts' must be a list")
    return [_parse_forecast(e, i) for i, e in enumerate(entries)]
