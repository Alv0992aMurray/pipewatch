"""Alert dispatch: route alerts to named destinations with filtering."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional
from pipewatch.alerts import Alert, AlertSeverity
from pipewatch.severity_map import severity_level


@dataclass
class DispatchTarget:
    name: str
    min_severity: AlertSeverity = AlertSeverity.WARNING
    pipelines: Optional[List[str]] = None  # None = all

    def accepts(self, alert: Alert) -> bool:
        if severity_level(alert.severity) < severity_level(self.min_severity):
            return False
        if self.pipelines is not None and alert.pipeline not in self.pipelines:
            return False
        return True


@dataclass
class DispatchRecord:
    target: str
    alert: Alert

    def to_dict(self) -> dict:
        return {
            "target": self.target,
            "pipeline": self.alert.pipeline,
            "rule": self.alert.rule,
            "severity": self.alert.severity.value,
            "message": self.alert.message,
        }


@dataclass
class DispatchResult:
    records: List[DispatchRecord] = field(default_factory=list)
    dropped: List[Alert] = field(default_factory=list)

    @property
    def total_dispatched(self) -> int:
        return len(self.records)

    @property
    def total_dropped(self) -> int:
        return len(self.dropped)

    def targets_hit(self) -> List[str]:
        return list({r.target for r in self.records})


def dispatch_alerts(
    alerts: List[Alert],
    targets: List[DispatchTarget],
) -> DispatchResult:
    result = DispatchResult()
    for alert in alerts:
        matched = False
        for target in targets:
            if target.accepts(alert):
                result.records.append(DispatchRecord(target=target.name, alert=alert))
                matched = True
        if not matched:
            result.dropped.append(alert)
    return result
