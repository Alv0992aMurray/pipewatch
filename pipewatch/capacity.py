"""Capacity planning: estimate how long until a metric breaches a threshold."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from pipewatch.history import PipelineHistory


@dataclass
class CapacityConfig:
    pipeline: str
    metric: str  # "success_rate" or "throughput"
    threshold: float  # breach level (e.g. 0.80 for success_rate)
    direction: str = "falling"  # "falling" | "rising"

    @property
    def window(self) -> int:
        return 10  # snapshots used for projection


@dataclass
class CapacityResult:
    pipeline: str
    metric: str
    current_value: Optional[float]
    threshold: float
    direction: str
    slope_per_run: Optional[float]
    runs_until_breach: Optional[int]
    will_breach: bool

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric": self.metric,
            "current_value": self.current_value,
            "threshold": self.threshold,
            "direction": self.direction,
            "slope_per_run": self.slope_per_run,
            "runs_until_breach": self.runs_until_breach,
            "will_breach": self.will_breach,
        }


def _get_values(history: PipelineHistory, metric: str, n: int) -> list[float]:
    snaps = history.last_n(n)
    out = []
    for s in snaps:
        v = s.success_rate if metric == "success_rate" else s.throughput
        if v is not None:
            out.append(v)
    return out


def _slope(values: list[float]) -> float:
    """Least-squares slope over evenly-spaced indices."""
    n = len(values)
    if n < 2:
        return 0.0
    x_mean = (n - 1) / 2
    y_mean = sum(values) / n
    num = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(values))
    den = sum((i - x_mean) ** 2 for i in range(n))
    return num / den if den else 0.0


def estimate_capacity(config: CapacityConfig, history: PipelineHistory) -> CapacityResult:
    values = _get_values(history, config.metric, config.window)

    if not values:
        return CapacityResult(
            pipeline=config.pipeline, metric=config.metric,
            current_value=None, threshold=config.threshold,
            direction=config.direction, slope_per_run=None,
            runs_until_breach=None, will_breach=False,
        )

    current = values[-1]
    slope = _slope(values)

    runs: Optional[int] = None
    will_breach = False

    if config.direction == "falling" and slope < 0:
        gap = current - config.threshold
        if gap > 0:
            runs = max(1, int(gap / abs(slope)))
            will_breach = True
    elif config.direction == "rising" and slope > 0:
        gap = config.threshold - current
        if gap > 0:
            runs = max(1, int(gap / slope))
            will_breach = True

    return CapacityResult(
        pipeline=config.pipeline, metric=config.metric,
        current_value=current, threshold=config.threshold,
        direction=config.direction, slope_per_run=round(slope, 6),
        runs_until_breach=runs, will_breach=will_breach,
    )
