"""Incident tracking: group related alerts into named incidents with lifecycle management."""

from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.alerts import Alert, AlertSeverity


def _now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class Incident:
    incident_id: str
    pipeline: str
    title: str
    severity: AlertSeverity
    opened_at: datetime
    alerts: List[Alert] = field(default_factory=list)
    resolved_at: Optional[datetime] = None

    @property
    def is_open(self) -> bool:
        return self.resolved_at is None

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.resolved_at is None:
            return (_now() - self.opened_at).total_seconds()
        return (self.resolved_at - self.opened_at).total_seconds()

    def resolve(self) -> None:
        if self.resolved_at is None:
            self.resolved_at = _now()

    def to_dict(self) -> dict:
        return {
            "incident_id": self.incident_id,
            "pipeline": self.pipeline,
            "title": self.title,
            "severity": self.severity.value,
            "opened_at": self.opened_at.isoformat(),
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None,
            "alert_count": len(self.alerts),
            "is_open": self.is_open,
        }


def _incident_key(pipeline: str, rule_name: str) -> str:
    raw = f"{pipeline}:{rule_name}"
    return hashlib.sha1(raw.encode()).hexdigest()[:12]


class IncidentTracker:
    """Tracks open incidents and groups incoming alerts."""

    def __init__(self) -> None:
        self._open: dict[str, Incident] = {}
        self._closed: List[Incident] = []

    def process(self, alerts: List[Alert]) -> List[Incident]:
        """Open new incidents for new alerts; return all currently open incidents."""
        for alert in alerts:
            key = _incident_key(alert.pipeline, alert.rule_name)
            if key not in self._open:
                incident = Incident(
                    incident_id=str(uuid.uuid4()),
                    pipeline=alert.pipeline,
                    title=f"{alert.rule_name} on {alert.pipeline}",
                    severity=alert.severity,
                    opened_at=_now(),
                    alerts=[alert],
                )
                self._open[key] = incident
            else:
                self._open[key].alerts.append(alert)
        return list(self._open.values())

    def resolve(self, pipeline: str, rule_name: str) -> Optional[Incident]:
        key = _incident_key(pipeline, rule_name)
        incident = self._open.pop(key, None)
        if incident:
            incident.resolve()
            self._closed.append(incident)
        return incident

    def open_incidents(self) -> List[Incident]:
        return list(self._open.values())

    def closed_incidents(self) -> List[Incident]:
        return list(self._closed)
