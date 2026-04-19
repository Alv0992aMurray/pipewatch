"""Cluster pipelines by similarity in health metrics."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Dict, Optional
from pipewatch.metrics import PipelineMetric


@dataclass
class ClusterEntry:
    pipeline: str
    success_rate: float
    throughput: float
    error_rate: float

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "success_rate": round(self.success_rate, 4),
            "throughput": round(self.throughput, 4),
            "error_rate": round(self.error_rate, 4),
        }


@dataclass
class Cluster:
    label: str
    entries: List[ClusterEntry] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.entries)

    @property
    def avg_success_rate(self) -> float:
        if not self.entries:
            return 0.0
        return sum(e.success_rate for e in self.entries) / len(self.entries)

    def to_dict(self) -> dict:
        return {
            "label": self.label,
            "count": self.count,
            "avg_success_rate": round(self.avg_success_rate, 4),
            "pipelines": [e.pipeline for e in self.entries],
        }


@dataclass
class ClusteringResult:
    clusters: List[Cluster]

    @property
    def total_pipelines(self) -> int:
        return sum(c.count for c in self.clusters)

    def to_dict(self) -> dict:
        return {
            "total_pipelines": self.total_pipelines,
            "clusters": [c.to_dict() for c in self.clusters],
        }


def _entry_from_metric(metric: PipelineMetric) -> ClusterEntry:
    total = metric.rows_processed
    success_rate = metric.rows_succeeded / total if total > 0 else 0.0
    error_rate = metric.rows_failed / total if total > 0 else 0.0
    throughput = total / max(metric.duration_seconds, 1)
    return ClusterEntry(
        pipeline=metric.pipeline_name,
        success_rate=success_rate,
        throughput=throughput,
        error_rate=error_rate,
    )


def cluster_metrics(metrics: List[PipelineMetric]) -> Optional[ClusteringResult]:
    """Cluster pipelines into healthy, degraded, and failing groups."""
    if not metrics:
        return None

    healthy = Cluster(label="healthy")
    degraded = Cluster(label="degraded")
    failing = Cluster(label="failing")

    for metric in metrics:
        entry = _entry_from_metric(metric)
        if entry.success_rate >= 0.95:
            healthy.entries.append(entry)
        elif entry.success_rate >= 0.70:
            degraded.entries.append(entry)
        else:
            failing.entries.append(entry)

    return ClusteringResult(clusters=[healthy, degraded, failing])
