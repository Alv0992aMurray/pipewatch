"""Velocity: measures rate of change in success_rate over recent snapshots."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from pipewatch.history import PipelineHistory


@dataclass
class VelocityResult:
    pipeline: str
    window_size: int
    first_rate: float
    last_rate: float
    delta: float          # last - first
    per_step: float       # delta / (window_size - 1)
    label: str            # "improving" | "declining" | "stable"

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "window_size": self.window_size,
            "first_rate": round(self.first_rate, 4),
            "last_rate": round(self.last_rate, 4),
            "delta": round(self.delta, 4),
            "per_step": round(self.per_step, 4),
            "label": self.label,
        }


def _label(delta: float, threshold: float) -> str:
    if delta > threshold:
        return "improving"
    if delta < -threshold:
        return "declining"
    return "stable"


def compute_velocity(
    history: PipelineHistory,
    window: int = 5,
    stable_threshold: float = 0.02,
) -> Optional[VelocityResult]:
    """Return a VelocityResult for the most recent *window* snapshots.

    Returns None when there are fewer than 2 snapshots available.
    """
    snaps = history.last_n(window)
    if len(snaps) < 2:
        return None

    rates = [s.success_rate for s in snaps]
    first, last = rates[0], rates[-1]
    delta = last - first
    per_step = delta / (len(rates) - 1)

    return VelocityResult(
        pipeline=history.pipeline,
        window_size=len(rates),
        first_rate=first,
        last_rate=last,
        delta=delta,
        per_step=per_step,
        label=_label(delta, stable_threshold),
    )
