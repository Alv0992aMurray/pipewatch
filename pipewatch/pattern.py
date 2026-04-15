"""Pattern detection: identify recurring failure patterns across pipeline history."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.history import PipelineHistory, MetricSnapshot


@dataclass
class PatternResult:
    pipeline: str
    metric: str
    total_snapshots: int
    failure_runs: int
    consecutive_failures: int
    alternating: bool
    pattern_label: str
    note: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric": self.metric,
            "total_snapshots": self.total_snapshots,
            "failure_runs": self.failure_runs,
            "consecutive_failures": self.consecutive_failures,
            "alternating": self.alternating,
            "pattern_label": self.pattern_label,
            "note": self.note,
        }


def _get_health_sequence(history: PipelineHistory) -> List[bool]:
    """Return ordered list of healthy (True) / unhealthy (False) booleans."""
    return [snap.is_healthy for snap in history.snapshots]


def _count_consecutive_failures(seq: List[bool]) -> int:
    count = 0
    for healthy in reversed(seq):
        if not healthy:
            count += 1
        else:
            break
    return count


def _is_alternating(seq: List[bool], min_len: int = 4) -> bool:
    if len(seq) < min_len:
        return False
    tail = seq[-min_len:]
    return all(tail[i] != tail[i + 1] for i in range(len(tail) - 1))


def detect_pattern(history: PipelineHistory) -> Optional[PatternResult]:
    """Analyse a pipeline's history and classify its failure pattern."""
    if not history.snapshots:
        return None

    pipeline = history.snapshots[0].pipeline
    metric = history.snapshots[0].metric
    seq = _get_health_sequence(history)
    total = len(seq)
    failure_runs = sum(1 for h in seq if not h)
    consecutive = _count_consecutive_failures(seq)
    alternating = _is_alternating(seq)

    if consecutive >= 3:
        label = "sustained_failure"
        note = f"{consecutive} consecutive failures detected"
    elif alternating:
        label = "flapping"
        note = "Pipeline is alternating between healthy and unhealthy"
    elif failure_runs == 0:
        label = "stable_healthy"
        note = None
    elif failure_runs / total < 0.2:
        label = "occasional_failure"
        note = f"{failure_runs}/{total} runs failed"
    else:
        label = "degraded"
        note = f"{failure_runs}/{total} runs failed"

    return PatternResult(
        pipeline=pipeline,
        metric=metric,
        total_snapshots=total,
        failure_runs=failure_runs,
        consecutive_failures=consecutive,
        alternating=alternating,
        pattern_label=label,
        note=note,
    )
