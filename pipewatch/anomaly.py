"""Anomaly detection for pipeline metrics using simple statistical methods."""

from __future__ import annotations

from dataclasses import dataclass
from statistics import mean, stdev
from typing import List, Optional

from pipewatch.history import MetricSnapshot


@dataclass
class AnomalyResult:
    pipeline: str
    metric: str
    current_value: float
    mean: float
    std_dev: float
    z_score: float
    is_anomaly: bool
    message: str

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric": self.metric,
            "current_value": round(self.current_value, 4),
            "mean": round(self.mean, 4),
            "std_dev": round(self.std_dev, 4),
            "z_score": round(self.z_score, 4),
            "is_anomaly": self.is_anomaly,
            "message": self.message,
        }


def _get_values(snapshots: List[MetricSnapshot], metric: str) -> List[float]:
    """Extract numeric values for a given metric from snapshots."""
    mapping = {
        "success_rate": lambda s: s.success_rate,
        "throughput": lambda s: s.throughput,
        "error_count": lambda s: float(s.error_count),
    }
    if metric not in mapping:
        raise ValueError(f"Unknown metric: {metric!r}")
    return [mapping[metric](s) for s in snapshots]


def detect_anomaly(
    snapshots: List[MetricSnapshot],
    metric: str = "success_rate",
    threshold: float = 2.0,
) -> Optional[AnomalyResult]:
    """Detect if the most recent snapshot is anomalous vs historical baseline.

    Uses z-score method. Requires at least 3 historical snapshots.
    Returns None if there is insufficient data.
    """
    if len(snapshots) < 3:
        return None

    values = _get_values(snapshots, metric)
    history, current = values[:-1], values[-1]

    if len(history) < 2:
        return None

    mu = mean(history)
    sigma = stdev(history)

    if sigma == 0.0:
        z = 0.0
    else:
        z = (current - mu) / sigma

    is_anomaly = abs(z) >= threshold
    pipeline = snapshots[-1].pipeline

    if is_anomaly:
        direction = "above" if z > 0 else "below"
        msg = (
            f"{metric} value {current:.4f} is {direction} normal range "
            f"(z={z:.2f}, threshold={threshold})"
        )
    else:
        msg = f"{metric} value {current:.4f} is within normal range (z={z:.2f})"

    return AnomalyResult(
        pipeline=pipeline,
        metric=metric,
        current_value=current,
        mean=mu,
        std_dev=sigma,
        z_score=z,
        is_anomaly=is_anomaly,
        message=msg,
    )
