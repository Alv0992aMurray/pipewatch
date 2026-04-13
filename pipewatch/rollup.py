"""Rollup: aggregate metrics across multiple pipelines into summary statistics."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.metrics import PipelineMetric, is_healthy, success_rate, throughput


@dataclass
class RollupStats:
    pipeline_count: int
    total_rows_processed: int
    total_errors: int
    avg_success_rate: Optional[float]  # None if no pipelines
    min_success_rate: Optional[float]
    max_success_rate: Optional[float]
    avg_throughput: Optional[float]
    healthy_count: int
    unhealthy_count: int

    def to_dict(self) -> dict:
        return {
            "pipeline_count": self.pipeline_count,
            "total_rows_processed": self.total_rows_processed,
            "total_errors": self.total_errors,
            "avg_success_rate": self.avg_success_rate,
            "min_success_rate": self.min_success_rate,
            "max_success_rate": self.max_success_rate,
            "avg_throughput": self.avg_throughput,
            "healthy_count": self.healthy_count,
            "unhealthy_count": self.unhealthy_count,
        }

    @property
    def health_ratio(self) -> Optional[float]:
        """Fraction of pipelines that are healthy (0.0–1.0), or None if no pipelines."""
        if self.pipeline_count == 0:
            return None
        return self.healthy_count / self.pipeline_count


def compute_rollup(metrics: List[PipelineMetric]) -> RollupStats:
    """Compute aggregate statistics across a list of pipeline metrics."""
    if not metrics:
        return RollupStats(
            pipeline_count=0,
            total_rows_processed=0,
            total_errors=0,
            avg_success_rate=None,
            min_success_rate=None,
            max_success_rate=None,
            avg_throughput=None,
            healthy_count=0,
            unhealthy_count=0,
        )

    rates = [r for m in metrics if (r := success_rate(m)) is not None]
    throughputs = [t for m in metrics if (t := throughput(m)) is not None]

    healthy = sum(1 for m in metrics if is_healthy(m))

    return RollupStats(
        pipeline_count=len(metrics),
        total_rows_processed=sum(m.rows_processed for m in metrics),
        total_errors=sum(m.rows_failed for m in metrics),
        avg_success_rate=sum(rates) / len(rates) if rates else None,
        min_success_rate=min(rates) if rates else None,
        max_success_rate=max(rates) if rates else None,
        avg_throughput=sum(throughputs) / len(throughputs) if throughputs else None,
        healthy_count=healthy,
        unhealthy_count=len(metrics) - healthy,
    )
