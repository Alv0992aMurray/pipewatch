"""Gap detection: identify missing snapshots in a pipeline's history."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Optional

from pipewatch.history import PipelineHistory


@dataclass
class GapInterval:
    pipeline: str
    start: datetime
    end: datetime
    duration_seconds: float

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "duration_seconds": round(self.duration_seconds, 2),
        }


@dataclass
class GapResult:
    pipeline: str
    expected_interval_seconds: float
    gaps: List[GapInterval] = field(default_factory=list)
    checked_snapshots: int = 0

    @property
    def has_gaps(self) -> bool:
        return len(self.gaps) > 0

    @property
    def total_gap_seconds(self) -> float:
        return sum(g.duration_seconds for g in self.gaps)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "expected_interval_seconds": self.expected_interval_seconds,
            "checked_snapshots": self.checked_snapshots,
            "gap_count": len(self.gaps),
            "total_gap_seconds": round(self.total_gap_seconds, 2),
            "gaps": [g.to_dict() for g in self.gaps],
        }


def detect_gaps(
    history: PipelineHistory,
    expected_interval: timedelta,
    tolerance: float = 1.5,
) -> Optional[GapResult]:
    """Detect gaps between consecutive snapshots exceeding tolerance * expected_interval."""
    snaps = history.last_n(1000)
    if len(snaps) < 2:
        return None

    pipeline = snaps[0].pipeline
    threshold = expected_interval.total_seconds() * tolerance
    gaps: List[GapInterval] = []

    for i in range(1, len(snaps)):
        prev = snaps[i - 1]
        curr = snaps[i]
        delta = (curr.timestamp - prev.timestamp).total_seconds()
        if delta > threshold:
            gaps.append(
                GapInterval(
                    pipeline=pipeline,
                    start=prev.timestamp,
                    end=curr.timestamp,
                    duration_seconds=delta,
                )
            )

    return GapResult(
        pipeline=pipeline,
        expected_interval_seconds=expected_interval.total_seconds(),
        gaps=gaps,
        checked_snapshots=len(snaps),
    )
