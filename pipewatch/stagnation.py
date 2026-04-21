"""Stagnation detection: identifies pipelines where metrics have not changed
over a configurable window, which may indicate a stuck or frozen pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from pipewatch.history import PipelineHistory


@dataclass
class StagnationConfig:
    pipeline: str
    min_snapshots: int = 5
    _window: int = 10  # number of recent snapshots to inspect
    tolerance: float = 0.0001  # max allowed variance to still be "stagnant"

    @property
    def window(self) -> int:
        return self._window


@dataclass
class StagnationResult:
    pipeline: str
    is_stagnant: bool
    snapshot_count: int
    unique_values: int
    variance: Optional[float]
    note: str = ""

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "is_stagnant": self.is_stagnant,
            "snapshot_count": self.snapshot_count,
            "unique_values": self.unique_values,
            "variance": self.variance,
            "note": self.note,
        }


def _get_values(history: PipelineHistory, n: int) -> list[float]:
    snaps = history.last_n(n)
    return [s.success_rate for s in snaps if s.success_rate is not None]


def _variance(values: list[float]) -> float:
    if not values:
        return 0.0
    mean = sum(values) / len(values)
    return sum((v - mean) ** 2 for v in values) / len(values)


def detect_stagnation(
    history: PipelineHistory, config: StagnationConfig
) -> Optional[StagnationResult]:
    values = _get_values(history, config.window)
    if len(values) < config.min_snapshots:
        return StagnationResult(
            pipeline=config.pipeline,
            is_stagnant=False,
            snapshot_count=len(values),
            unique_values=len(set(values)),
            variance=None,
            note="insufficient data",
        )

    var = _variance(values)
    unique = len(set(round(v, 6) for v in values))
    stagnant = var <= config.tolerance

    return StagnationResult(
        pipeline=config.pipeline,
        is_stagnant=stagnant,
        snapshot_count=len(values),
        unique_values=unique,
        variance=var,
        note="metric appears frozen" if stagnant else "",
    )
