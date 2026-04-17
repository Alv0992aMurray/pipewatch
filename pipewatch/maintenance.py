"""Maintenance window support — suppress alerts during planned downtime."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

from pipewatch.alerts import Alert


def _now() -> datetime:
    return datetime.utcnow()


@dataclass
class MaintenanceWindow:
    pipeline: str  # '*' matches all pipelines
    start: datetime
    end: datetime
    reason: str = ""

    def is_active(self, at: Optional[datetime] = None) -> bool:
        t = at or _now()
        return self.start <= t <= self.end

    def matches(self, alert: Alert) -> bool:
        return self.pipeline == "*" or self.pipeline == alert.pipeline

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "reason": self.reason,
            "active": self.is_active(),
        }


@dataclass
class MaintenanceResult:
    kept: List[Alert] = field(default_factory=list)
    suppressed: List[Alert] = field(default_factory=list)

    @property
    def total_suppressed(self) -> int:
        return len(self.suppressed)


def apply_maintenance(
    alerts: List[Alert],
    windows: List[MaintenanceWindow],
    at: Optional[datetime] = None,
) -> MaintenanceResult:
    result = MaintenanceResult()
    active = [w for w in windows if w.is_active(at)]
    for alert in alerts:
        if any(w.matches(alert) for w in active):
            result.suppressed.append(alert)
        else:
            result.kept.append(alert)
    return result
