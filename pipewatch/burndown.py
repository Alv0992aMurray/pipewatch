"""Track alert burndown — how quickly open alerts are being resolved over time."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional


@dataclass
class BurndownPoint:
    timestamp: datetime
    open_count: int
    resolved_count: int

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp.isoformat(),
            "open_count": self.open_count,
            "resolved_count": self.resolved_count,
        }


@dataclass
class BurndownResult:
    pipeline: str
    points: List[BurndownPoint] = field(default_factory=list)

    @property
    def total_opened(self) -> int:
        return sum(p.open_count for p in self.points)

    @property
    def total_resolved(self) -> int:
        return sum(p.resolved_count for p in self.points)

    @property
    def resolution_rate(self) -> Optional[float]:
        if self.total_opened == 0:
            return None
        return round(self.total_resolved / self.total_opened, 4)

    @property
    def trend(self) -> str:
        if len(self.points) < 2:
            return "insufficient_data"
        first = self.points[0].open_count
        last = self.points[-1].open_count
        if last < first:
            return "improving"
        if last > first:
            return "worsening"
        return "stable"

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "total_opened": self.total_opened,
            "total_resolved": self.total_resolved,
            "resolution_rate": self.resolution_rate,
            "trend": self.trend,
            "points": [p.to_dict() for p in self.points],
        }


def compute_burndown(
    pipeline: str,
    events: List[dict],
) -> BurndownResult:
    """Build a BurndownResult from a list of timestamped event dicts.

    Each event dict must have: 'timestamp' (datetime), 'opened' (int), 'resolved' (int).
    """
    result = BurndownResult(pipeline=pipeline)
    for ev in sorted(events, key=lambda e: e["timestamp"]):
        result.points.append(
            BurndownPoint(
                timestamp=ev["timestamp"],
                open_count=ev.get("opened", 0),
                resolved_count=ev.get("resolved", 0),
            )
        )
    return result
