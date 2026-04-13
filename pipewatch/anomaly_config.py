"""Load anomaly detection configuration from YAML."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List

import yaml

_DEFAULT_CONFIG = "pipewatch_alerts.yml"
_VALID_METRICS = {"success_rate", "throughput", "error_count"}


class AnomalyConfigError(Exception):
    """Raised when anomaly configuration is invalid."""


@dataclass
class AnomalyConfig:
    pipeline: str
    metric: str = "success_rate"
    threshold: float = 2.0
    min_history: int = 3


def _parse_anomaly(raw: dict) -> AnomalyConfig:
    if "pipeline" not in raw:
        raise AnomalyConfigError("Each anomaly entry must specify 'pipeline'.")
    metric = raw.get("metric", "success_rate")
    if metric not in _VALID_METRICS:
        raise AnomalyConfigError(
            f"Invalid metric {metric!r}. Choose from {sorted(_VALID_METRICS)}."
        )
    threshold = float(raw.get("threshold", 2.0))
    if threshold <= 0:
        raise AnomalyConfigError("'threshold' must be a positive number.")
    return AnomalyConfig(
        pipeline=raw["pipeline"],
        metric=metric,
        threshold=threshold,
        min_history=int(raw.get("min_history", 3)),
    )


def load_anomaly_configs(path: str = _DEFAULT_CONFIG) -> List[AnomalyConfig]:
    """Load anomaly detection configs from a YAML file."""
    config_path = Path(path)
    if not config_path.exists():
        raise AnomalyConfigError(f"Config file not found: {path}")

    with config_path.open() as fh:
        data = yaml.safe_load(fh)

    if not data or "anomalies" not in data:
        return []

    return [_parse_anomaly(entry) for entry in data["anomalies"]]
