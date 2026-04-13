"""Track and persist pipeline metric history for trend analysis."""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.metrics import PipelineMetric

DEFAULT_HISTORY_FILE = ".pipewatch_history.json"


@dataclass
class MetricSnapshot:
    """A timestamped snapshot of a pipeline metric."""

    pipeline: str
    timestamp: str
    success_rate: float
    throughput: float
    error_count: int
    is_healthy: bool

    @classmethod
    def from_metric(cls, metric: PipelineMetric) -> "MetricSnapshot":
        from pipewatch.metrics import success_rate, throughput, is_healthy

        return cls(
            pipeline=metric.pipeline_name,
            timestamp=datetime.now(timezone.utc).isoformat(),
            success_rate=round(success_rate(metric), 4),
            throughput=round(throughput(metric), 4),
            error_count=metric.error_count,
            is_healthy=is_healthy(metric),
        )


@dataclass
class PipelineHistory:
    """Stores a list of snapshots for a single pipeline."""

    pipeline: str
    snapshots: List[MetricSnapshot] = field(default_factory=list)

    def add(self, snapshot: MetricSnapshot) -> None:
        self.snapshots.append(snapshot)

    def last_n(self, n: int) -> List[MetricSnapshot]:
        return self.snapshots[-n:]

    def average_success_rate(self, last_n: Optional[int] = None) -> Optional[float]:
        entries = self.last_n(last_n) if last_n else self.snapshots
        if not entries:
            return None
        return round(sum(s.success_rate for s in entries) / len(entries), 4)


def load_history(path: str = DEFAULT_HISTORY_FILE) -> dict[str, PipelineHistory]:
    """Load history from a JSON file. Returns empty dict if file missing."""
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as fh:
        raw = json.load(fh)
    result: dict[str, PipelineHistory] = {}
    for pipeline, data in raw.items():
        snapshots = [MetricSnapshot(**s) for s in data.get("snapshots", [])]
        result[pipeline] = PipelineHistory(pipeline=pipeline, snapshots=snapshots)
    return result


def save_history(
    history: dict[str, PipelineHistory], path: str = DEFAULT_HISTORY_FILE
) -> None:
    """Persist history to a JSON file."""
    serialisable = {
        name: {"pipeline": h.pipeline, "snapshots": [asdict(s) for s in h.snapshots]}
        for name, h in history.items()
    }
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(serialisable, fh, indent=2)


def record_metric(
    metric: PipelineMetric, path: str = DEFAULT_HISTORY_FILE
) -> MetricSnapshot:
    """Append a metric snapshot to history and persist it."""
    history = load_history(path)
    snapshot = MetricSnapshot.from_metric(metric)
    if metric.pipeline_name not in history:
        history[metric.pipeline_name] = PipelineHistory(pipeline=metric.pipeline_name)
    history[metric.pipeline_name].add(snapshot)
    save_history(history, path)
    return snapshot
