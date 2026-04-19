"""Jitter detection: measures timing variability between pipeline snapshots."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, List
import statistics

from pipewatch.history import PipelineHistory


@dataclass
class JitterResult:
    pipeline: str
    sample_count: int
    mean_interval_seconds: float
    stddev_seconds: float
    jitter_ratio: float          # stddev / mean; 0 = perfectly regular
    is_irregular: bool
    note: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "sample_count": self.sample_count,
            "mean_interval_seconds": round(self.mean_interval_seconds, 3),
            "stddev_seconds": round(self.stddev_seconds, 3),
            "jitter_ratio": round(self.jitter_ratio, 4),
            "is_irregular": self.is_irregular,
            "note": self.note,
        }


def _get_intervals(history: PipelineHistory) -> List[float]:
    snaps = history.last_n(len(history.snapshots))
    if len(snaps) < 2:
        return []
    timestamps = sorted(s.timestamp for s in snaps)
    return [
        (timestamps[i + 1] - timestamps[i]).total_seconds()
        for i in range(len(timestamps) - 1)
    ]


def detect_jitter(
    history: PipelineHistory,
    irregularity_threshold: float = 0.3,
    min_samples: int = 3,
) -> Optional[JitterResult]:
    """Return a JitterResult or None if there is insufficient data."""
    intervals = _get_intervals(history)
    if len(intervals) < min_samples:
        return None

    mean = statistics.mean(intervals)
    if mean == 0:
        return None

    stddev = statistics.pstdev(intervals)
    ratio = stddev / mean
    irregular = ratio > irregularity_threshold

    note: Optional[str] = None
    if irregular:
        note = f"High timing variability detected (ratio={ratio:.3f})"

    pipeline = history.snapshots[0].pipeline if history.snapshots else "unknown"
    return JitterResult(
        pipeline=pipeline,
        sample_count=len(intervals) + 1,
        mean_interval_seconds=mean,
        stddev_seconds=stddev,
        jitter_ratio=ratio,
        is_irregular=irregular,
        note=note,
    )
