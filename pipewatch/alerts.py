"""Alert definitions and evaluation logic for pipeline health monitoring."""

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional

from pipewatch.metrics import PipelineMetric, success_rate, throughput, is_healthy


class AlertSeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass
class AlertRule:
    """Defines a threshold-based alert rule for a pipeline metric."""

    name: str
    metric: str  # 'success_rate' | 'throughput' | 'error_count'
    threshold: float
    operator: str  # 'lt' | 'gt' | 'lte' | 'gte'
    severity: AlertSeverity = AlertSeverity.WARNING
    message: Optional[str] = None

    def evaluate(self, pipeline_metric: PipelineMetric) -> Optional["Alert"]:
        """Return an Alert if the rule is triggered, otherwise None."""
        value = self._extract_value(pipeline_metric)
        if value is None:
            return None

        triggered = (
            (self.operator == "lt" and value < self.threshold)
            or (self.operator == "gt" and value > self.threshold)
            or (self.operator == "lte" and value <= self.threshold)
            or (self.operator == "gte" and value >= self.threshold)
        )

        if triggered:
            msg = self.message or (
                f"{self.metric} is {value:.2f}, "
                f"threshold {self.operator} {self.threshold}"
            )
            return Alert(
                rule_name=self.name,
                severity=self.severity,
                pipeline=pipeline_metric.pipeline_name,
                metric=self.metric,
                value=value,
                message=msg,
            )
        return None

    def _extract_value(self, m: PipelineMetric) -> Optional[float]:
        if self.metric == "success_rate":
            return success_rate(m)
        if self.metric == "throughput":
            return throughput(m)
        if self.metric == "error_count":
            return float(m.error_count)
        return None


@dataclass
class Alert:
    """Represents a fired alert."""

    rule_name: str
    severity: AlertSeverity
    pipeline: str
    metric: str
    value: float
    message: str

    def to_dict(self) -> dict:
        return {
            "rule": self.rule_name,
            "severity": self.severity.value,
            "pipeline": self.pipeline,
            "metric": self.metric,
            "value": self.value,
            "message": self.message,
        }


def evaluate_rules(
    metric: PipelineMetric, rules: List[AlertRule]
) -> List[Alert]:
    """Evaluate all rules against a metric and return any triggered alerts."""
    return [alert for rule in rules for alert in [rule.evaluate(metric)] if alert]
