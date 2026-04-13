"""Notification routing: map alerts to named channels with optional filtering."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.alerts import Alert, AlertSeverity


@dataclass
class NotificationChannel:
    """A named destination that receives alerts meeting a severity threshold."""

    name: str
    min_severity: AlertSeverity = AlertSeverity.WARNING
    pipeline_filter: Optional[str] = None  # None means all pipelines

    def accepts(self, alert: Alert) -> bool:
        """Return True if *alert* should be routed to this channel."""
        if alert.severity.value < self.min_severity.value:
            return False
        if self.pipeline_filter is not None:
            if alert.pipeline != self.pipeline_filter:
                return False
        return True


@dataclass
class NotificationResult:
    """Outcome of routing a batch of alerts through all channels."""

    routed: dict[str, List[Alert]] = field(default_factory=dict)

    def total_routed(self) -> int:
        return sum(len(v) for v in self.routed.values())

    def channels_with_alerts(self) -> List[str]:
        return [ch for ch, alerts in self.routed.items() if alerts]

    def to_dict(self) -> dict:
        return {
            ch: [a.to_dict() for a in alerts]
            for ch, alerts in self.routed.items()
        }


def route_alerts(
    alerts: List[Alert],
    channels: List[NotificationChannel],
) -> NotificationResult:
    """Route *alerts* to each channel that accepts them."""
    result = NotificationResult()
    for channel in channels:
        result.routed[channel.name] = [
            a for a in alerts if channel.accepts(a)
        ]
    return result
