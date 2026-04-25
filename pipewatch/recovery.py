"""Recovery detection: identifies pipelines transitioning from unhealthy to healthy."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.history import PipelineHistory


@dataclass
class RecoveryResult:
    pipeline: str
    recovered: bool
    previous_failures: int
    recovery_snapshot_index: Optional[int]
    note: str

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "recovered": self.recovered,
            "previous_failures": self.previous_failures,
            "recovery_snapshot_index": self.recovery_snapshot_index,
            "note": self.note,
        }


def _get_health_sequence(history: PipelineHistory) -> List[bool]:
    return [s.healthy for s in history.snapshots]


def detect_recovery(
    history: PipelineHistory,
    min_prior_failures: int = 1,
) -> Optional[RecoveryResult]:
    """Return a RecoveryResult if the most recent snapshot shows a recovery.

    A recovery is defined as the latest snapshot being healthy after at least
    ``min_prior_failures`` consecutive unhealthy snapshots immediately before it.
    """
    snaps = history.snapshots
    if len(snaps) < 2:
        return None

    sequence = _get_health_sequence(history)

    # Latest must be healthy
    if not sequence[-1]:
        return RecoveryResult(
            pipeline=history.pipeline,
            recovered=False,
            previous_failures=0,
            recovery_snapshot_index=None,
            note="latest snapshot is not healthy",
        )

    # Count consecutive failures immediately before the latest snapshot
    consecutive_failures = 0
    for health in reversed(sequence[:-1]):
        if not health:
            consecutive_failures += 1
        else:
            break

    recovered = consecutive_failures >= min_prior_failures
    return RecoveryResult(
        pipeline=history.pipeline,
        recovered=recovered,
        previous_failures=consecutive_failures,
        recovery_snapshot_index=len(snaps) - 1 if recovered else None,
        note=(
            f"recovered after {consecutive_failures} consecutive failure(s)"
            if recovered
            else f"only {consecutive_failures} prior failure(s), threshold is {min_prior_failures}"
        ),
    )
