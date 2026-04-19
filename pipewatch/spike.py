"""Spike detection: identify sudden increases in a metric relative to recent history."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, List
from pipewatch.history import PipelineHistory


@dataclass
class SpikeResult:
    pipeline: str
    metric: str
    current_value: float
    baseline_mean: float
    ratio: float
    is_spike: bool
    note: str = ""

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric": self.metric,
            "current_value": self.current_value,
            "baseline_mean": self.baseline_mean,
            "ratio": self.ratio,
            "is_spike": self.is_spike,
            "note": self.note,
        }


def _get_values(history: PipelineHistory, metric: str) -> List[float]:
    results = []
    for snap in history.snapshots:
        v = snap.to_dict().get(metric)
        if v is not None:
            results.append(float(v))
    return results


def _mean(values: List[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def detect_spike(
    history: PipelineHistory,
    metric: str = "error_rate",
    ratio_threshold: float = 2.0,
    min_snapshots: int = 5,
) -> Optional[SpikeResult]:
    """Detect if the latest value is a spike compared to the rolling baseline."""
    values = _get_values(history, metric)
    if len(values) < min_snapshots:
        return None

    current = values[-1]
    baseline_values = values[:-1]
    baseline_mean = _mean(baseline_values)

    if baseline_mean == 0.0:
        ratio = 1.0
        is_spike = False
        note = "baseline mean is zero; cannot compute ratio"
    else:
        ratio = current / baseline_mean
        is_spike = ratio >= ratio_threshold
        note = f"ratio {ratio:.2f} exceeds threshold {ratio_threshold}" if is_spike else ""

    return SpikeResult(
        pipeline=history.pipeline_name,
        metric=metric,
        current_value=current,
        baseline_mean=baseline_mean,
        ratio=ratio,
        is_spike=is_spike,
        note=note,
    )
