"""Regression detection: compare current metric against a recent baseline window."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from pipewatch.history import PipelineHistory


@dataclass
class RegressionResult:
    pipeline: str
    metric: str
    current_value: float
    baseline_mean: float
    pct_change: float          # negative = degradation
    regressed: bool
    threshold_pct: float
    sample_size: int

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric": self.metric,
            "current_value": round(self.current_value, 4),
            "baseline_mean": round(self.baseline_mean, 4),
            "pct_change": round(self.pct_change, 4),
            "regressed": self.regressed,
            "threshold_pct": self.threshold_pct,
            "sample_size": self.sample_size,
        }


def _mean(values: list[float]) -> float:
    return sum(values) / len(values)


def _get_values(history: PipelineHistory, metric: str) -> list[float]:
    results = []
    for snap in history.snapshots:
        v = snap.to_dict().get(metric)
        if v is not None:
            results.append(float(v))
    return results


def detect_regression(
    history: PipelineHistory,
    metric: str = "success_rate",
    baseline_window: int = 10,
    threshold_pct: float = 0.10,
) -> Optional[RegressionResult]:
    """Detect if the latest value has regressed beyond *threshold_pct* relative
    to the mean of the previous *baseline_window* snapshots.

    Returns None when there is insufficient history.
    """
    values = _get_values(history, metric)
    # Need at least baseline_window + 1 points (baseline + one current)
    if len(values) < baseline_window + 1:
        return None

    baseline_values = values[-(baseline_window + 1):-1]
    current_value = values[-1]
    baseline_mean = _mean(baseline_values)

    if baseline_mean == 0.0:
        pct_change = 0.0
    else:
        pct_change = (current_value - baseline_mean) / baseline_mean

    regressed = pct_change < -threshold_pct

    return RegressionResult(
        pipeline=history.pipeline_name,
        metric=metric,
        current_value=current_value,
        baseline_mean=baseline_mean,
        pct_change=pct_change,
        regressed=regressed,
        threshold_pct=threshold_pct,
        sample_size=len(baseline_values),
    )
