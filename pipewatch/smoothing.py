"""Exponential moving average smoothing for pipeline metrics."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.history import PipelineHistory


_DEFAULT_ALPHA = 0.3  # smoothing factor; 0 < alpha <= 1


@dataclass
class SmoothedPoint:
    index: int
    raw: float
    smoothed: float

    def to_dict(self) -> dict:
        return {"index": self.index, "raw": self.raw, "smoothed": round(self.smoothed, 6)}


@dataclass
class SmoothingResult:
    pipeline: str
    metric: str
    alpha: float
    points: List[SmoothedPoint] = field(default_factory=list)
    insufficient_data: bool = False

    def latest_smoothed(self) -> Optional[float]:
        return self.points[-1].smoothed if self.points else None

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric": self.metric,
            "alpha": self.alpha,
            "insufficient_data": self.insufficient_data,
            "latest_smoothed": self.latest_smoothed(),
            "points": [p.to_dict() for p in self.points],
        }


def _get_values(history: PipelineHistory, metric: str) -> List[float]:
    """Extract a numeric series from snapshot history."""
    out: List[float] = []
    for snap in history.snapshots:
        val = snap.to_dict().get(metric)
        if val is not None:
            out.append(float(val))
    return out


def smooth(
    history: PipelineHistory,
    metric: str = "success_rate",
    alpha: float = _DEFAULT_ALPHA,
    min_points: int = 2,
) -> Optional[SmoothingResult]:
    """Apply exponential moving average to *metric* values in *history*.

    Returns ``None`` when history is empty; sets ``insufficient_data`` when
    fewer than *min_points* values are present.
    """
    values = _get_values(history, metric)
    pipeline = history.pipeline_name

    if not values:
        return None

    result = SmoothingResult(pipeline=pipeline, metric=metric, alpha=alpha)

    if len(values) < min_points:
        result.insufficient_data = True
        return result

    ema = values[0]
    for i, raw in enumerate(values):
        if i == 0:
            ema = raw
        else:
            ema = alpha * raw + (1.0 - alpha) * ema
        result.points.append(SmoothedPoint(index=i, raw=raw, smoothed=ema))

    return result
