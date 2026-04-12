"""Pipeline run orchestration: evaluate rules against a metric and produce a report."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from pipewatch.alerts import Alert, AlertRule, evaluate
from pipewatch.metrics import PipelineMetric
from pipewatch.reporter import build_report, format_alerts, format_metric_summary


@dataclass
class RunResult:
    """Holds the outcome of a single pipeline evaluation run."""

    metric: PipelineMetric
    alerts: List[Alert] = field(default_factory=list)
    report: dict = field(default_factory=dict)

    @property
    def has_critical(self) -> bool:
        from pipewatch.alerts import AlertSeverity
        return any(a.severity == AlertSeverity.CRITICAL for a in self.alerts)

    @property
    def has_alerts(self) -> bool:
        return len(self.alerts) > 0


def run_checks(metric: PipelineMetric, rules: List[AlertRule]) -> RunResult:
    """Evaluate all alert rules against a metric and return a RunResult."""
    triggered: List[Alert] = []
    for rule in rules:
        alert = evaluate(rule, metric)
        if alert is not None:
            triggered.append(alert)

    report = build_report(metric, triggered)
    return RunResult(metric=metric, alerts=triggered, report=report)


def print_run_result(result: RunResult) -> None:
    """Print a human-readable summary of a RunResult to stdout."""
    print(format_metric_summary(result.metric))
    print(format_alerts(result.alerts))
