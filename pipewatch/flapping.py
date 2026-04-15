"""Flapping detection: identifies pipelines that oscillate between healthy and unhealthy states."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.history import PipelineHistory


@dataclass
class FlappingResult:
    pipeline: str
    window_size: int
    transitions: int
    is_flapping: bool
    health_sequence: List[bool] = field(default_factory=list)
    note: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "window_size": self.window_size,
            "transitions": self.transitions,
            "is_flapping": self.is_flapping,
            "health_sequence": self.health_sequence,
            "note": self.note,
        }


def _get_health_sequence(history: PipelineHistory, window: int) -> List[bool]:
    snapshots = history.last_n(window)
    return [s.is_healthy for s in snapshots]


def _count_transitions(sequence: List[bool]) -> int:
    if len(sequence) < 2:
        return 0
    return sum(
        1 for i in range(1, len(sequence))
        if sequence[i] != sequence[i - 1]
    )


def detect_flapping(
    history: PipelineHistory,
    window: int = 10,
    min_transitions: int = 3,
) -> Optional[FlappingResult]:
    """Detect whether a pipeline is flapping within the last *window* snapshots.

    Returns None if there is insufficient data (fewer than 2 snapshots).
    """
    sequence = _get_health_sequence(history, window)

    if len(sequence) < 2:
        return None

    transitions = _count_transitions(sequence)
    is_flapping = transitions >= min_transitions

    note: Optional[str] = None
    if is_flapping:
        note = (
            f"Pipeline changed health state {transitions} time(s) "
            f"in the last {len(sequence)} snapshots."
        )

    return FlappingResult(
        pipeline=history.pipeline,
        window_size=window,
        transitions=transitions,
        is_flapping=is_flapping,
        health_sequence=sequence,
        note=note,
    )
