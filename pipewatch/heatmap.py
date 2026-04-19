"""Pipeline health heatmap: buckets pipelines by hour-of-day health patterns."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from pipewatch.history import PipelineHistory, MetricSnapshot


@dataclass
class HeatmapCell:
    hour: int
    sample_count: int
    avg_success_rate: float
    healthy_count: int
    unhealthy_count: int

    def to_dict(self) -> dict:
        return {
            "hour": self.hour,
            "sample_count": self.sample_count,
            "avg_success_rate": round(self.avg_success_rate, 4),
            "healthy_count": self.healthy_count,
            "unhealthy_count": self.unhealthy_count,
        }


@dataclass
class HeatmapResult:
    pipeline: str
    cells: List[HeatmapCell] = field(default_factory=list)

    def worst_hour(self) -> Optional[int]:
        if not self.cells:
            return None
        return min(self.cells, key=lambda c: c.avg_success_rate).hour

    def best_hour(self) -> Optional[int]:
        if not self.cells:
            return None
        return max(self.cells, key=lambda c: c.avg_success_rate).hour

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "worst_hour": self.worst_hour(),
            "best_hour": self.best_hour(),
            "cells": [c.to_dict() for c in self.cells],
        }


def build_heatmap(history: PipelineHistory) -> Optional[HeatmapResult]:
    snapshots = history.last_n(10_000)
    if not snapshots:
        return None

    buckets: Dict[int, List[MetricSnapshot]] = {}
    for snap in snapshots:
        hour = snap.timestamp.hour
        buckets.setdefault(hour, []).append(snap)

    cells = []
    for hour in sorted(buckets):
        snaps = buckets[hour]
        rates = [s.success_rate for s in snaps]
        healthy = sum(1 for s in snaps if s.is_healthy)
        cells.append(HeatmapCell(
            hour=hour,
            sample_count=len(snaps),
            avg_success_rate=sum(rates) / len(rates),
            healthy_count=healthy,
            unhealthy_count=len(snaps) - healthy,
        ))

    return HeatmapResult(pipeline=history.pipeline, cells=cells)
