"""Alert inhibition: suppress child alerts when a parent alert is firing."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional
from pipewatch.alerts import Alert, AlertSeverity


@dataclass
class InhibitionRule:
    source_pipeline: str
    source_severity: AlertSeverity
    target_pipeline: str  # "*" matches any
    reason: str = ""

    def source_matches(self, alert: Alert) -> bool:
        return (
            alert.pipeline == self.source_pipeline
            and alert.severity == self.source_severity
        )

    def target_matches(self, alert: Alert) -> bool:
        if self.target_pipeline == "*":
            return True
        return alert.pipeline == self.target_pipeline


@dataclass
class InhibitionResult:
    kept: List[Alert] = field(default_factory=list)
    inhibited: List[Alert] = field(default_factory=list)

    @property
    def total_inhibited(self) -> int:
        return len(self.inhibited)

    def to_dict(self) -> dict:
        return {
            "kept": [a.to_dict() for a in self.kept],
            "inhibited": [a.to_dict() for a in self.inhibited],
            "total_inhibited": self.total_inhibited,
        }


def apply_inhibition(
    alerts: List[Alert],
    rules: List[InhibitionRule],
) -> InhibitionResult:
    """Suppress target alerts when a matching source alert is present.

    An alert is inhibited if an active rule's source alert is firing and
    the alert matches the rule's target pipeline. Source alerts themselves
    are never inhibited by the rule that triggered them.
    """
    active_rules = [
        rule for rule in rules
        if any(rule.source_matches(a) for a in alerts)
    ]

    result = InhibitionResult()
    for alert in alerts:
        inhibited_by: Optional[InhibitionRule] = None
        for rule in active_rules:
            if rule.target_matches(alert) and not rule.source_matches(alert):
                inhibited_by = rule
                break
        if inhibited_by:
            result.inhibited.append(alert)
        else:
            result.kept.append(alert)
    return result
