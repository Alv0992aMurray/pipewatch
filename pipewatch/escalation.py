"""Alert escalation: promote alert severity after repeated firing."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

from pipewatch.alerts import Alert, AlertSeverity


@dataclass
class EscalationPolicy:
    """Defines when an alert should be escalated to a higher severity."""

    pipeline: str
    rule_name: str
    # Number of consecutive firings before escalation
    threshold: int = 3
    # Severity to escalate to
    escalate_to: AlertSeverity = AlertSeverity.CRITICAL


@dataclass
class EscalationEntry:
    """Tracks consecutive firings for a single pipeline/rule pair."""

    pipeline: str
    rule_name: str
    count: int = 0
    last_fired: Optional[datetime] = None

    def record(self, when: Optional[datetime] = None) -> None:
        self.count += 1
        self.last_fired = when or datetime.now(timezone.utc)

    def reset(self) -> None:
        self.count = 0
        self.last_fired = None

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "rule_name": self.rule_name,
            "count": self.count,
            "last_fired": self.last_fired.isoformat() if self.last_fired else None,
        }


@dataclass
class EscalationResult:
    """Result of applying escalation policies to a list of alerts."""

    alerts: List[Alert] = field(default_factory=list)
    escalated: List[Alert] = field(default_factory=list)

    @property
    def total_escalated(self) -> int:
        return len(self.escalated)


class AlertEscalator:
    """Tracks alert firing history and escalates when thresholds are met."""

    def __init__(self) -> None:
        self._entries: Dict[str, EscalationEntry] = {}

    def _key(self, pipeline: str, rule_name: str) -> str:
        return f"{pipeline}::{rule_name}"

    def _get_or_create(self, pipeline: str, rule_name: str) -> EscalationEntry:
        k = self._key(pipeline, rule_name)
        if k not in self._entries:
            self._entries[k] = EscalationEntry(pipeline=pipeline, rule_name=rule_name)
        return self._entries[k]

    def process(
        self,
        alerts: List[Alert],
        policies: List[EscalationPolicy],
        when: Optional[datetime] = None,
    ) -> EscalationResult:
        policy_map: Dict[str, EscalationPolicy] = {
            self._key(p.pipeline, p.rule_name): p for p in policies
        }

        result_alerts: List[Alert] = []
        escalated: List[Alert] = []

        for alert in alerts:
            k = self._key(alert.pipeline, alert.rule_name)
            entry = self._get_or_create(alert.pipeline, alert.rule_name)
            entry.record(when)

            policy = policy_map.get(k)
            if policy and entry.count >= policy.threshold:
                upgraded = Alert(
                    pipeline=alert.pipeline,
                    rule_name=alert.rule_name,
                    severity=policy.escalate_to,
                    message=f"[ESCALATED] {alert.message}",
                    value=alert.value,
                )
                result_alerts.append(upgraded)
                escalated.append(upgraded)
            else:
                result_alerts.append(alert)

        return EscalationResult(alerts=result_alerts, escalated=escalated)

    def reset(self, pipeline: str, rule_name: str) -> None:
        """Reset the firing counter (e.g. after alert clears)."""
        k = self._key(pipeline, rule_name)
        if k in self._entries:
            self._entries[k].reset()

    def entries(self) -> List[EscalationEntry]:
        return list(self._entries.values())
