"""Metric drift detection: compare recent window against a reference window."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.history import PipelineHistory, MetricSnapshot


@dataclass
class DriftResult:
    pipeline: str
    metric: str  # 'success_rate' or 'error_rate'
    reference_mean: Optional[float]
    recent_mean: Optional[float]
    delta: Optional[float]          # recent - reference
    relative_change: Optional[float]  # delta / reference_mean
    drifted: bool
    threshold: float

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric": self.metric,
            "reference_mean": self.reference_mean,
            "recent_mean": self.recent_mean,
            "delta": self.delta,
            "relative_change": self.relative_change,
            "drifted": self.drifted,
            "threshold": self.threshold,
        }


def _mean(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return sum(values) / len(values)


def _get_values(history: PipelineHistory, metric: str, n: int) -> List[float]:
    snaps: List[MetricSnapshot] = history.last_n(n)
    result = []
    for s in snaps:
        v = getattr(s, metric, None)
        if v is not None:
            result.append(v)
    return result


def detect_drift(
    history: PipelineHistory,
    metric: str = "success_rate",
    reference_window: int = 10,
    recent_window: int = 5,
    threshold: float = 0.10,
) -> Optional[DriftResult]:
    """Detect if the recent window has drifted from the reference window.

    Returns None when there is insufficient data.
    Drift is flagged when |relative_change| >= threshold.
    """
    min_required = reference_window + recent_window
    all_snaps = history.last_n(min_required)
    if len(all_snaps) < min_required:
        return None

    ref_snaps = all_snaps[:reference_window]
    recent_snaps = all_snaps[reference_window:]

    ref_values = [getattr(s, metric) for s in ref_snaps if getattr(s, metric, None) is not None]
    recent_values = [getattr(s, metric) for s in recent_snaps if getattr(s, metric, None) is not None]

    ref_mean = _mean(ref_values)
    recent_mean = _mean(recent_values)

    if ref_mean is None or recent_mean is None:
        return None

    delta = recent_mean - ref_mean
    relative_change = delta / ref_mean if ref_mean != 0.0 else None
    drifted = relative_change is not None and abs(relative_change) >= threshold

    return DriftResult(
        pipeline=history.pipeline_name,
        metric=metric,
        reference_mean=ref_mean,
        recent_mean=recent_mean,
        delta=delta,
        relative_change=relative_change,
        drifted=drifted,
        threshold=threshold,
    )
