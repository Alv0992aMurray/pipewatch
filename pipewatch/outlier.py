"""Outlier detection for pipeline metrics using IQR-based method."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional
from pipewatch.history import PipelineHistory


@dataclass
class OutlierResult:
    pipeline: str
    metric: str
    value: float
    mean: float
    lower_fence: float
    upper_fence: float
    is_outlier: bool
    direction: Optional[str] = None  # "high" | "low" | None

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric": self.metric,
            "value": self.value,
            "mean": round(self.mean, 4),
            "lower_fence": round(self.lower_fence, 4),
            "upper_fence": round(self.upper_fence, 4),
            "is_outlier": self.is_outlier,
            "direction": self.direction,
        }


def _get_values(history: PipelineHistory, metric: str) -> list[float]:
    values = []
    for snap in history.snapshots:
        v = getattr(snap, metric, None)
        if v is not None:
            values.append(v)
    return values


def _iqr_fences(values: list[float], multiplier: float = 1.5) -> tuple[float, float]:
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    q1 = sorted_vals[n // 4]
    q3 = sorted_vals[(3 * n) // 4]
    iqr = q3 - q1
    return q1 - multiplier * iqr, q3 + multiplier * iqr


def detect_outlier(
    history: PipelineHistory,
    metric: str = "success_rate",
    min_samples: int = 5,
    multiplier: float = 1.5,
) -> Optional[OutlierResult]:
    values = _get_values(history, metric)
    if len(values) < min_samples:
        return None

    latest = values[-1]
    mean = sum(values) / len(values)
    lower, upper = _iqr_fences(values, multiplier)

    is_outlier = latest < lower or latest > upper
    direction: Optional[str] = None
    if is_outlier:
        direction = "high" if latest > upper else "low"

    name = history.snapshots[-1].pipeline if history.snapshots else "unknown"
    return OutlierResult(
        pipeline=name,
        metric=metric,
        value=latest,
        mean=mean,
        lower_fence=lower,
        upper_fence=upper,
        is_outlier=is_outlier,
        direction=direction,
    )
