"""Maps alert severities to numeric levels and supports comparison utilities."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List

from pipewatch.alerts import Alert, AlertSeverity

_SEVERITY_ORDER: Dict[AlertSeverity, int] = {
    AlertSeverity.INFO: 0,
    AlertSeverity.WARNING: 1,
    AlertSeverity.CRITICAL: 2,
}


def severity_level(severity: AlertSeverity) -> int:
    """Return a numeric level for a severity (higher = more severe)."""
    return _SEVERITY_ORDER.get(severity, 0)


def max_severity(alerts: List[Alert]) -> AlertSeverity | None:
    """Return the highest severity found in a list of alerts, or None if empty."""
    if not alerts:
        return None
    return max(alerts, key=lambda a: severity_level(a.severity)).severity


def filter_by_min_severity(
    alerts: List[Alert], min_severity: AlertSeverity
) -> List[Alert]:
    """Return only alerts at or above the given minimum severity."""
    min_level = severity_level(min_severity)
    return [a for a in alerts if severity_level(a.severity) >= min_level]


@dataclass
class SeveritySummary:
    info: int = 0
    warning: int = 0
    critical: int = 0

    def total(self) -> int:
        return self.info + self.warning + self.critical

    def to_dict(self) -> Dict[str, int]:
        return {
            "info": self.info,
            "warning": self.warning,
            "critical": self.critical,
            "total": self.total(),
        }


def summarise_severities(alerts: List[Alert]) -> SeveritySummary:
    """Count alerts by severity level."""
    summary = SeveritySummary()
    for alert in alerts:
        if alert.severity == AlertSeverity.INFO:
            summary.info += 1
        elif alert.severity == AlertSeverity.WARNING:
            summary.warning += 1
        elif alert.severity == AlertSeverity.CRITICAL:
            summary.critical += 1
    return summary
