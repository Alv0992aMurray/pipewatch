"""Percentile analysis for pipeline success rate history."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.history import PipelineHistory


@dataclass
class PercentileResult:
    pipeline: str
    metric: str
    sample_count: int
    p50: Optional[float]
    p90: Optional[float]
    p95: Optional[float]
    p99: Optional[float]
    insufficient_data: bool = False

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric": self.metric,
            "sample_count": self.sample_count,
            "p50": self.p50,
            "p90": self.p90,
            "p95": self.p95,
            "p99": self.p99,
            "insufficient_data": self.insufficient_data,
        }


MIN_SAMPLES = 5


def _get_values(history: PipelineHistory, metric: str) -> List[float]:
    values = []
    for snap in history.snapshots:
        v = snap.data.get(metric)
        if v is not None:
            values.append(float(v))
    return values


def _percentile(sorted_values: List[float], p: float) -> float:
    """Nearest-rank percentile."""
    n = len(sorted_values)
    rank = max(1, int(round(p / 100.0 * n)))
    return sorted_values[min(rank, n) - 1]


def compute_percentiles(
    history: PipelineHistory,
    metric: str = "success_rate",
) -> Optional[PercentileResult]:
    """Compute p50/p90/p95/p99 for *metric* across all snapshots in *history*."""
    if not history.snapshots:
        return None

    values = _get_values(history, metric)
    pipeline = history.snapshots[0].pipeline

    if len(values) < MIN_SAMPLES:
        return PercentileResult(
            pipeline=pipeline,
            metric=metric,
            sample_count=len(values),
            p50=None,
            p90=None,
            p95=None,
            p99=None,
            insufficient_data=True,
        )

    sv = sorted(values)
    return PercentileResult(
        pipeline=pipeline,
        metric=metric,
        sample_count=len(sv),
        p50=_percentile(sv, 50),
        p90=_percentile(sv, 90),
        p95=_percentile(sv, 95),
        p99=_percentile(sv, 99),
        insufficient_data=False,
    )
