"""Latency tracking: measures time between consecutive snapshots."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

from pipewatch.history import PipelineHistory


@dataclass
class LatencyResult:
    pipeline: str
    sample_count: int
    min_seconds: Optional[float]
    max_seconds: Optional[float]
    avg_seconds: Optional[float]
    is_high: bool
    threshold_seconds: float

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "sample_count": self.sample_count,
            "min_seconds": self.min_seconds,
            "max_seconds": self.max_seconds,
            "avg_seconds": self.avg_seconds,
            "is_high": self.is_high,
            "threshold_seconds": self.threshold_seconds,
        }


def _get_intervals(history: PipelineHistory) -> List[float]:
    snaps = history.last_n(len(history.snapshots))
    if len(snaps) < 2:
        return []
    intervals = []
    for i in range(1, len(snaps)):
        delta = (snaps[i].timestamp - snaps[i - 1].timestamp).total_seconds()
        if delta >= 0:
            intervals.append(delta)
    return intervals


def detect_latency(
    history: PipelineHistory,
    threshold_seconds: float = 300.0,
) -> Optional[LatencyResult]:
    intervals = _get_intervals(history)
    if not intervals:
        return None

    avg = sum(intervals) / len(intervals)
    return LatencyResult(
        pipeline=history.pipeline,
        sample_count=len(intervals),
        min_seconds=min(intervals),
        max_seconds=max(intervals),
        avg_seconds=avg,
        is_high=avg > threshold_seconds,
        threshold_seconds=threshold_seconds,
    )
