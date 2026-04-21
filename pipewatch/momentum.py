"""Momentum analysis: measures rate-of-change acceleration in pipeline health metrics."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, List

from pipewatch.history import PipelineHistory, MetricSnapshot


@dataclass
class MomentumResult:
    pipeline: str
    metric: str
    snapshots_used: int
    first_velocity: Optional[float]   # change per step in first half
    second_velocity: Optional[float]  # change per step in second half
    acceleration: Optional[float]     # second_velocity - first_velocity
    label: str                        # "accelerating" | "decelerating" | "stable" | "insufficient data"
    sufficient_data: bool

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric": self.metric,
            "snapshots_used": self.snapshots_used,
            "first_velocity": self.first_velocity,
            "second_velocity": self.second_velocity,
            "acceleration": self.acceleration,
            "label": self.label,
            "sufficient_data": self.sufficient_data,
        }


def _get_values(snapshots: List[MetricSnapshot], metric: str) -> List[float]:
    out = []
    for snap in snapshots:
        v = snap.success_rate if metric == "success_rate" else snap.throughput
        if v is not None:
            out.append(v)
    return out


def _velocity(values: List[float]) -> Optional[float]:
    """Average step-to-step change across a sequence."""
    if len(values) < 2:
        return None
    deltas = [values[i + 1] - values[i] for i in range(len(values) - 1)]
    return sum(deltas) / len(deltas)


def _classify(acceleration: float, threshold: float = 0.005) -> str:
    if acceleration > threshold:
        return "accelerating"
    if acceleration < -threshold:
        return "decelerating"
    return "stable"


def detect_momentum(
    history: PipelineHistory,
    metric: str = "success_rate",
    min_snapshots: int = 6,
) -> MomentumResult:
    """Split history in half and compare velocities to compute acceleration."""
    snaps = history.last_n(min_snapshots * 2)  # grab up to 2x for headroom
    values = _get_values(snaps, metric)
    n = len(values)

    if n < min_snapshots:
        return MomentumResult(
            pipeline=history.pipeline,
            metric=metric,
            snapshots_used=n,
            first_velocity=None,
            second_velocity=None,
            acceleration=None,
            label="insufficient data",
            sufficient_data=False,
        )

    mid = n // 2
    first_half = values[:mid]
    second_half = values[mid:]

    v1 = _velocity(first_half)
    v2 = _velocity(second_half)
    accel = (v2 - v1) if (v1 is not None and v2 is not None) else None
    label = _classify(accel) if accel is not None else "insufficient data"

    return MomentumResult(
        pipeline=history.pipeline,
        metric=metric,
        snapshots_used=n,
        first_velocity=v1,
        second_velocity=v2,
        acceleration=accel,
        label=label,
        sufficient_data=True,
    )
