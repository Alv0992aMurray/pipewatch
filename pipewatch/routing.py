"""Alert routing: direct alerts to named destinations based on pipeline tags and severity."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.alerts import Alert, AlertSeverity


@dataclass
class RoutingRule:
    """Maps a set of conditions to a destination name."""

    destination: str
    pipeline: Optional[str] = None          # None means match any pipeline
    min_severity: AlertSeverity = AlertSeverity.WARNING

    def matches(self, alert: Alert) -> bool:
        """Return True when *alert* satisfies this rule's conditions."""
        if self.pipeline is not None and alert.pipeline != self.pipeline:
            return False
        severity_order = list(AlertSeverity)
        return severity_order.index(alert.severity) >= severity_order.index(self.min_severity)


@dataclass
class RoutingResult:
    """Outcome of routing a single alert."""

    alert: Alert
    destinations: List[str] = field(default_factory=list)

    @property
    def routed(self) -> bool:
        return len(self.destinations) > 0

    def to_dict(self) -> dict:
        return {
            "pipeline": self.alert.pipeline,
            "rule": self.alert.rule,
            "severity": self.alert.severity.value,
            "destinations": self.destinations,
            "routed": self.routed,
        }


def route_alerts(
    alerts: List[Alert],
    rules: List[RoutingRule],
) -> List[RoutingResult]:
    """Route each alert through all matching rules and return results."""
    results: List[RoutingResult] = []
    for alert in alerts:
        destinations = [
            r.destination for r in rules if r.matches(alert)
        ]
        results.append(RoutingResult(alert=alert, destinations=destinations))
    return results
