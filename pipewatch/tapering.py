"""Tapering detection: identifies when a pipeline's metric is gradually
declining toward a threshold, even if not yet in breach."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, List

from pipewatch.history import PipelineHistory, MetricSnapshot


@dataclass
class TaperingResult:
    pipeline: str
    metric: str
    is_tapering: bool
    slope: Optional[float]          # change per snapshot (negative = declining)
    current_value: Optional[float]
    projected_breach: Optional[int]  # snapshots until threshold breach (None = no breach projected)
    threshold: float
    insufficient_data: bool = False

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric": self.metric,
            "is_tapering": self.is_tapering,
            "slope": round(self.slope, 6) if self.slope is not None else None,
            "current_value": round(self.current_value, 4) if self.current_value is not None else None,
            "projected_breach": self.projected_breach,
            "threshold": self.threshold,
            "insufficient_data": self.insufficient_data,
        }


def _get_values(history: PipelineHistory, metric: str) -> List[float]:
    snapshots = history.last_n(history._max_size if hasattr(history, '_max_size') else 200)
    values = []
    for snap in snapshots:
        v = snap.to_dict().get(metric)
        if v is not None:
            values.append(float(v))
    return values


def _linear_slope(values: List[float]) -> float:
    n = len(values)
    x_mean = (n - 1) / 2.0
    y_mean = sum(values) / n
    numerator = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(values))
    denominator = sum((i - x_mean) ** 2 for i in range(n))
    if denominator == 0:
        return 0.0
    return numerator / denominator


def detect_tapering(
    history: PipelineHistory,
    pipeline: str,
    metric: str = "success_rate",
    threshold: float = 0.90,
    min_snapshots: int = 5,
    slope_threshold: float = -0.005,
) -> Optional[TaperingResult]:
    """Return a TaperingResult if the metric is on a declining trajectory.

    A pipeline is considered to be tapering when:
    - The slope of the metric over recent snapshots is below `slope_threshold`
    - The current value is above `threshold` (not yet breaching)
    """
    values = _get_values(history, metric)
    if not values:
        return None
    if len(values) < min_snapshots:
        return TaperingResult(
            pipeline=pipeline,
            metric=metric,
            is_tapering=False,
            slope=None,
            current_value=values[-1] if values else None,
            projected_breach=None,
            threshold=threshold,
            insufficient_data=True,
        )

    slope = _linear_slope(values)
    current = values[-1]
    is_tapering = slope < slope_threshold and current > threshold

    projected_breach: Optional[int] = None
    if is_tapering and slope < 0:
        steps = (current - threshold) / abs(slope)
        projected_breach = max(1, int(steps))

    return TaperingResult(
        pipeline=pipeline,
        metric=metric,
        is_tapering=is_tapering,
        slope=slope,
        current_value=current,
        projected_breach=projected_breach,
        threshold=threshold,
    )
