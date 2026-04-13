"""Baseline comparison: compare current metrics against a stored baseline."""

from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional

from pipewatch.metrics import PipelineMetric, success_rate, throughput


DEFAULT_BASELINE_PATH = Path("pipewatch_baseline.json")


@dataclass
class BaselineEntry:
    pipeline_id: str
    success_rate: float
    throughput: float

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(data: dict) -> "BaselineEntry":
        return BaselineEntry(
            pipeline_id=data["pipeline_id"],
            success_rate=data["success_rate"],
            throughput=data["throughput"],
        )


@dataclass
class BaselineDelta:
    pipeline_id: str
    success_rate_delta: float   # current - baseline (positive = improved)
    throughput_delta: float
    regressed: bool             # True if any metric worsened beyond tolerance


def capture_baseline(metric: PipelineMetric) -> BaselineEntry:
    """Create a BaselineEntry from a current metric snapshot."""
    return BaselineEntry(
        pipeline_id=metric.pipeline_id,
        success_rate=success_rate(metric),
        throughput=throughput(metric),
    )


def compare_to_baseline(
    metric: PipelineMetric,
    baseline: BaselineEntry,
    tolerance: float = 0.05,
) -> BaselineDelta:
    """Compare current metric against baseline; flag regression if any delta
    falls below *-tolerance* (e.g. 0.05 = 5 percentage points)."""
    sr_delta = success_rate(metric) - baseline.success_rate
    tp_delta = throughput(metric) - baseline.throughput
    regressed = sr_delta < -tolerance or tp_delta < -tolerance
    return BaselineDelta(
        pipeline_id=metric.pipeline_id,
        success_rate_delta=round(sr_delta, 4),
        throughput_delta=round(tp_delta, 4),
        regressed=regressed,
    )


def save_baseline(entry: BaselineEntry, path: Path = DEFAULT_BASELINE_PATH) -> None:
    existing: dict = {}
    if path.exists():
        existing = json.loads(path.read_text())
    existing[entry.pipeline_id] = entry.to_dict()
    path.write_text(json.dumps(existing, indent=2))


def load_baseline(
    pipeline_id: str, path: Path = DEFAULT_BASELINE_PATH
) -> Optional[BaselineEntry]:
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    entry_data = data.get(pipeline_id)
    if entry_data is None:
        return None
    return BaselineEntry.from_dict(entry_data)
