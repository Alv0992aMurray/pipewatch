"""Detects pipeline health churn — frequent transitions between healthy and unhealthy states."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional
from pipewatch.history import PipelineHistory


@dataclass
class ChurnResult:
    pipeline: str
    window_size: int
    transitions: int
    churn_rate: float  # transitions per snapshot
    is_churning: bool
    note: str = ""

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "window_size": self.window_size,
            "transitions": self.transitions,
            "churn_rate": round(self.churn_rate, 4),
            "is_churning": self.is_churning,
            "note": self.note,
        }


def _get_health_sequence(history: PipelineHistory, n: int) -> list[bool]:
    snapshots = history.last_n(n)
    return [s.is_healthy for s in snapshots]


def _count_transitions(sequence: list[bool]) -> int:
    if len(sequence) < 2:
        return 0
    return sum(1 for a, b in zip(sequence, sequence[1:]) if a != b)


def detect_churn(
    history: PipelineHistory,
    window: int = 10,
    threshold: float = 0.4,
) -> Optional[ChurnResult]:
    """Return a ChurnResult if enough data exists, else None."""
    snapshots = history.last_n(window)
    if len(snapshots) < 3:
        return None

    sequence = [s.is_healthy for s in snapshots]
    transitions = _count_transitions(sequence)
    churn_rate = transitions / max(len(sequence) - 1, 1)
    is_churning = churn_rate >= threshold

    note = ""
    if is_churning:
        note = f"High churn detected: {transitions} transitions in last {len(sequence)} snapshots."

    return ChurnResult(
        pipeline=history.pipeline,
        window_size=len(sequence),
        transitions=transitions,
        churn_rate=churn_rate,
        is_churning=is_churning,
        note=note,
    )
