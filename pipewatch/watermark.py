"""High-water mark tracking for pipeline metrics."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

from pipewatch.history import MetricSnapshot


@dataclass
class WatermarkEntry:
    pipeline: str
    metric: str
    peak_value: float
    recorded_at: datetime
    snapshot_count: int = 1

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric": self.metric,
            "peak_value": round(self.peak_value, 4),
            "recorded_at": self.recorded_at.isoformat(),
            "snapshot_count": self.snapshot_count,
        }


@dataclass
class WatermarkResult:
    entries: List[WatermarkEntry] = field(default_factory=list)

    def get(self, pipeline: str, metric: str) -> Optional[WatermarkEntry]:
        for e in self.entries:
            if e.pipeline == pipeline and e.metric == metric:
                return e
        return None

    def to_dict(self) -> dict:
        return {"watermarks": [e.to_dict() for e in self.entries]}


def _get_metric_value(snap: MetricSnapshot, metric: str) -> Optional[float]:
    if metric == "success_rate":
        return snap.success_rate
    if metric == "throughput":
        return snap.throughput
    if metric == "error_rate":
        return snap.error_rate
    return None


def compute_watermarks(
    snapshots: List[MetricSnapshot],
    metrics: Optional[List[str]] = None,
) -> WatermarkResult:
    if metrics is None:
        metrics = ["success_rate", "throughput"]

    peaks: Dict[tuple, WatermarkEntry] = {}

    for snap in snapshots:
        for metric in metrics:
            value = _get_metric_value(snap, metric)
            if value is None:
                continue
            key = (snap.pipeline, metric)
            if key not in peaks or value > peaks[key].peak_value:
                peaks[key] = WatermarkEntry(
                    pipeline=snap.pipeline,
                    metric=metric,
                    peak_value=value,
                    recorded_at=snap.timestamp,
                )
            else:
                peaks[key].snapshot_count += 1

    return WatermarkResult(entries=list(peaks.values()))
