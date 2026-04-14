"""Time-window based alert silencing for pipewatch."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

from pipewatch.alerts import Alert


@dataclass
class SilenceWindow:
    """A named window during which matching alerts are silenced."""

    name: str
    pipeline: str          # '*' matches any pipeline
    start: datetime
    end: datetime
    reason: str = ""

    def is_active(self, now: Optional[datetime] = None) -> bool:
        now = now or datetime.utcnow()
        return self.start <= now <= self.end

    def matches(self, alert: Alert, now: Optional[datetime] = None) -> bool:
        if not self.is_active(now):
            return False
        if self.pipeline != "*" and self.pipeline != alert.pipeline:
            return False
        return True

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "pipeline": self.pipeline,
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "reason": self.reason,
            "active": self.is_active(),
        }


@dataclass
class SilenceResult:
    kept: List[Alert] = field(default_factory=list)
    silenced: List[Alert] = field(default_factory=list)

    @property
    def total_silenced(self) -> int:
        return len(self.silenced)


def apply_silences(
    alerts: List[Alert],
    windows: List[SilenceWindow],
    now: Optional[datetime] = None,
) -> SilenceResult:
    """Filter *alerts* through active silence windows."""
    result = SilenceResult()
    for alert in alerts:
        matched = any(w.matches(alert, now) for w in windows)
        if matched:
            result.silenced.append(alert)
        else:
            result.kept.append(alert)
    return result
