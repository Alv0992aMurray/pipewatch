"""Pipeline lifecycle state tracking — open, degraded, recovering, healthy."""
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional
from pipewatch.history import PipelineHistory


def _now() -> datetime:
    return datetime.now(timezone.utc)


LIFECYCLE_STATES = ("healthy", "degraded", "recovering", "unknown")


@dataclass
class LifecycleState:
    pipeline: str
    state: str
    since: datetime
    previous: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "state": self.state,
            "since": self.since.isoformat(),
            "previous": self.previous,
        }


@dataclass
class LifecycleResult:
    states: List[LifecycleState] = field(default_factory=list)

    def get(self, pipeline: str) -> Optional[LifecycleState]:
        for s in self.states:
            if s.pipeline == pipeline:
                return s
        return None

    def to_dict(self) -> dict:
        return {"states": [s.to_dict() for s in self.states]}


def _infer_state(history: PipelineHistory, window: int = 5) -> str:
    snaps = history.last_n(window)
    if not snaps:
        return "unknown"
    healths = [s.is_healthy for s in snaps]
    if all(healths):
        return "healthy"
    if not any(healths):
        return "degraded"
    # recovering: last snapshot healthy but not all
    if healths[-1]:
        return "recovering"
    return "degraded"


def evaluate_lifecycle(
    histories: List[PipelineHistory],
    previous: Optional[LifecycleResult] = None,
    window: int = 5,
) -> LifecycleResult:
    result = LifecycleResult()
    prev_map = {s.pipeline: s for s in (previous.states if previous else [])}
    now = _now()
    for history in histories:
        name = history.pipeline
        state = _infer_state(history, window)
        prev_state = prev_map.get(name)
        if prev_state and prev_state.state == state:
            result.states.append(
                LifecycleState(name, state, prev_state.since, prev_state.previous)
            )
        else:
            result.states.append(
                LifecycleState(
                    name,
                    state,
                    now,
                    prev_state.state if prev_state else None,
                )
            )
    return result
