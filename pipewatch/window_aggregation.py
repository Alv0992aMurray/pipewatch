"""Sliding window aggregation over pipeline metric snapshots."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Optional

from pipewatch.history import MetricSnapshot, PipelineHistory


@dataclass
class WindowAggregationConfig:
    window_minutes: int = 60

    @property
    def window(self) -> timedelta:
        return timedelta(minutes=self.window_minutes)


@dataclass
class WindowAggregationResult:
    pipeline: str
    window_minutes: int
    snapshot_count: int
    avg_success_rate: Optional[float]
    min_success_rate: Optional[float]
    max_success_rate: Optional[float]
    avg_throughput: Optional[float]
    healthy_count: int
    unhealthy_count: int

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "window_minutes": self.window_minutes,
            "snapshot_count": self.snapshot_count,
            "avg_success_rate": self.avg_success_rate,
            "min_success_rate": self.min_success_rate,
            "max_success_rate": self.max_success_rate,
            "avg_throughput": self.avg_throughput,
            "healthy_count": self.healthy_count,
            "unhealthy_count": self.unhealthy_count,
        }

    @property
    def health_ratio(self) -> Optional[float]:
        if self.snapshot_count == 0:
            return None
        return self.healthy_count / self.snapshot_count


def aggregate_window(
    history: PipelineHistory,
    config: WindowAggregationConfig,
    now: Optional[datetime] = None,
) -> Optional[WindowAggregationResult]:
    if now is None:
        now = datetime.utcnow()

    cutoff = now - config.window
    snapshots: List[MetricSnapshot] = [
        s for s in history.snapshots if s.timestamp >= cutoff
    ]

    if not snapshots:
        return None

    rates = [s.success_rate for s in snapshots if s.success_rate is not None]
    throughputs = [s.throughput for s in snapshots if s.throughput is not None]
    healthy = sum(1 for s in snapshots if s.is_healthy)

    return WindowAggregationResult(
        pipeline=history.pipeline,
        window_minutes=config.window_minutes,
        snapshot_count=len(snapshots),
        avg_success_rate=sum(rates) / len(rates) if rates else None,
        min_success_rate=min(rates) if rates else None,
        max_success_rate=max(rates) if rates else None,
        avg_throughput=sum(throughputs) / len(throughputs) if throughputs else None,
        healthy_count=healthy,
        unhealthy_count=len(snapshots) - healthy,
    )
