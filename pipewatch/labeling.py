"""Label assignment for pipeline metrics based on configurable rules."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class LabelRule:
    """A rule that assigns a label when a metric condition is met."""

    label: str
    tag_key: str
    tag_value: str
    min_success_rate: Optional[float] = None  # inclusive lower bound
    max_success_rate: Optional[float] = None  # inclusive upper bound

    def matches(self, metric: PipelineMetric) -> bool:
        """Return True when the metric satisfies this rule's conditions."""
        if metric.tags.get(self.tag_key) != self.tag_value:
            return False
        rate = metric.success_rate()
        if self.min_success_rate is not None and rate < self.min_success_rate:
            return False
        if self.max_success_rate is not None and rate > self.max_success_rate:
            return False
        return True


@dataclass
class LabelResult:
    """Outcome of applying label rules to a single metric."""

    pipeline: str
    labels: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict:
        return {"pipeline": self.pipeline, "labels": list(self.labels)}


def apply_labels(
    metric: PipelineMetric,
    rules: List[LabelRule],
) -> LabelResult:
    """Apply all matching label rules to *metric* and return a LabelResult."""
    assigned: List[str] = []
    for rule in rules:
        if rule.matches(metric):
            if rule.label not in assigned:
                assigned.append(rule.label)
    return LabelResult(pipeline=metric.pipeline_name, labels=assigned)


def label_metrics(
    metrics: List[PipelineMetric],
    rules: List[LabelRule],
) -> List[LabelResult]:
    """Apply label rules to every metric and return one result per metric."""
    return [apply_labels(m, rules) for m in metrics]
