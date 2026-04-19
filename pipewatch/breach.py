"""Threshold breach tracking: records when metrics cross defined limits."""
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional


@dataclass
class BreachConfig:
    metric: str  # 'success_rate' or 'error_rate'
    threshold: float
    direction: str = "below"  # 'below' or 'above'

    @property
    def window(self) -> str:
        return f"{self.direction} {self.threshold}"


@dataclass
class BreachEvent:
    pipeline: str
    metric: str
    value: float
    threshold: float
    direction: str
    timestamp: datetime

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric": self.metric,
            "value": round(self.value, 4),
            "threshold": self.threshold,
            "direction": self.direction,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class BreachResult:
    pipeline: str
    config: BreachConfig
    events: List[BreachEvent] = field(default_factory=list)

    @property
    def total_breaches(self) -> int:
        return len(self.events)

    @property
    def latest(self) -> Optional[BreachEvent]:
        return self.events[-1] if self.events else None

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric": self.config.metric,
            "threshold": self.config.threshold,
            "direction": self.config.direction,
            "total_breaches": self.total_breaches,
            "latest": self.latest.to_dict() if self.latest else None,
        }


def _get_value(snapshot, metric: str) -> Optional[float]:
    return getattr(snapshot, metric, None)


def detect_breaches(history, config: BreachConfig) -> Optional[BreachResult]:
    from pipewatch.history import PipelineHistory
    snapshots = history.last_n(len(history.snapshots))
    if not snapshots:
        return None
    result = BreachResult(pipeline=history.pipeline, config=config)
    for snap in snapshots:
        val = _get_value(snap, config.metric)
        if val is None:
            continue
        breached = (
            (config.direction == "below" and val < config.threshold) or
            (config.direction == "above" and val > config.threshold)
        )
        if breached:
            result.events.append(BreachEvent(
                pipeline=history.pipeline,
                metric=config.metric,
                value=val,
                threshold=config.threshold,
                direction=config.direction,
                timestamp=snap.timestamp,
            ))
    return result
