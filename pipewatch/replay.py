"""Replay historical pipeline metric snapshots through alert rules."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.alerts import AlertRule, Alert, evaluate
from pipewatch.history import PipelineHistory, MetricSnapshot
from pipewatch.metrics import PipelineMetric


@dataclass
class ReplayEvent:
    """A single alert evaluation against a historical snapshot."""

    snapshot: MetricSnapshot
    alerts: List[Alert]

    def has_alerts(self) -> bool:
        return len(self.alerts) > 0

    def to_dict(self) -> dict:
        return {
            "timestamp": self.snapshot.timestamp.isoformat(),
            "pipeline": self.snapshot.pipeline_name,
            "alerts": [
                {"rule": a.rule_name, "severity": a.severity.value, "message": a.message}
                for a in self.alerts
            ],
        }


@dataclass
class ReplayResult:
    """Aggregated result of replaying a history against a set of rules."""

    pipeline_name: str
    events: List[ReplayEvent] = field(default_factory=list)

    @property
    def total_snapshots(self) -> int:
        return len(self.events)

    @property
    def total_alert_events(self) -> int:
        return sum(1 for e in self.events if e.has_alerts())

    @property
    def total_alerts(self) -> int:
        return sum(len(e.alerts) for e in self.events)


def _snapshot_to_metric(snap: MetricSnapshot) -> PipelineMetric:
    """Reconstruct a PipelineMetric from a MetricSnapshot for rule evaluation."""
    return PipelineMetric(
        pipeline_name=snap.pipeline_name,
        rows_processed=snap.rows_processed,
        rows_failed=snap.rows_failed,
        duration_seconds=snap.duration_seconds,
        tags=snap.tags,
    )


def replay_history(
    history: PipelineHistory,
    rules: List[AlertRule],
    last_n: Optional[int] = None,
) -> ReplayResult:
    """Replay *rules* against snapshots in *history*, returning a ReplayResult."""
    snapshots = history.last_n(last_n) if last_n is not None else history.last_n(len(history.snapshots))
    result = ReplayResult(pipeline_name=history.pipeline_name)
    for snap in snapshots:
        metric = _snapshot_to_metric(snap)
        alerts = [a for rule in rules for a in ([evaluate(rule, metric)] if evaluate(rule, metric) else [])]
        result.events.append(ReplayEvent(snapshot=snap, alerts=alerts))
    return result
