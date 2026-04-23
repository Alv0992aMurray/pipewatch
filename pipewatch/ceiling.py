"""Ceiling detection: flag when a metric appears to be hitting an upper bound.

A pipeline whose success_rate stays persistently near 1.0 or whose throughput
clusters tightly at a fixed maximum may be constrained by capacity or config.
This module detects that pattern and reports it.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from pipewatch.history import PipelineHistory

_DEFAULT_THRESHOLD = 0.98  # fraction of max considered "at ceiling"
_DEFAULT_MIN_SNAPSHOTS = 5
_DEFAULT_RATIO = 0.8  # proportion of window that must be at ceiling


@dataclass
class CeilingResult:
    pipeline: str
    metric: str
    at_ceiling: bool
    ceiling_value: Optional[float]
    ratio_at_ceiling: float  # fraction of snapshots at/above threshold
    sample_count: int
    insufficient_data: bool
    note: str = ""

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric": self.metric,
            "at_ceiling": self.at_ceiling,
            "ceiling_value": self.ceiling_value,
            "ratio_at_ceiling": round(self.ratio_at_ceiling, 4),
            "sample_count": self.sample_count,
            "insufficient_data": self.insufficient_data,
            "note": self.note,
        }


def _get_values(history: PipelineHistory, metric: str) -> list[float]:
    snaps = history.last_n(len(history.snapshots))
    result = []
    for s in snaps:
        v = s.to_dict().get(metric)
        if v is not None:
            try:
                result.append(float(v))
            except (TypeError, ValueError):
                pass
    return result


def detect_ceiling(
    history: PipelineHistory,
    metric: str = "success_rate",
    threshold: float = _DEFAULT_THRESHOLD,
    min_snapshots: int = _DEFAULT_MIN_SNAPSHOTS,
    required_ratio: float = _DEFAULT_RATIO,
) -> Optional[CeilingResult]:
    """Return a CeilingResult for *history* or None if history is empty."""
    values = _get_values(history, metric)
    pipeline = history.pipeline_name

    if not values:
        return None

    if len(values) < min_snapshots:
        return CeilingResult(
            pipeline=pipeline,
            metric=metric,
            at_ceiling=False,
            ceiling_value=None,
            ratio_at_ceiling=0.0,
            sample_count=len(values),
            insufficient_data=True,
            note="insufficient data",
        )

    max_val = max(values)
    cutoff = max_val * threshold if max_val > 0 else threshold
    at_ceiling_count = sum(1 for v in values if v >= cutoff)
    ratio = at_ceiling_count / len(values)
    at_ceiling = ratio >= required_ratio

    return CeilingResult(
        pipeline=pipeline,
        metric=metric,
        at_ceiling=at_ceiling,
        ceiling_value=round(max_val, 6),
        ratio_at_ceiling=ratio,
        sample_count=len(values),
        insufficient_data=False,
        note="metric appears constrained at ceiling" if at_ceiling else "",
    )
