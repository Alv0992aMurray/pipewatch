"""Pipeline metric grouping by arbitrary key functions."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class MetricGroup:
    """A named collection of metrics sharing a common group key."""

    key: str
    metrics: List[PipelineMetric] = field(default_factory=list)

    def count(self) -> int:
        return len(self.metrics)

    def healthy_count(self) -> int:
        from pipewatch.metrics import is_healthy
        return sum(1 for m in self.metrics if is_healthy(m))

    def unhealthy_count(self) -> int:
        return self.count() - self.healthy_count()

    def avg_success_rate(self) -> Optional[float]:
        from pipewatch.metrics import success_rate
        rates = [success_rate(m) for m in self.metrics if m.total_rows > 0]
        if not rates:
            return None
        return sum(rates) / len(rates)

    def to_dict(self) -> dict:
        return {
            "key": self.key,
            "count": self.count(),
            "healthy": self.healthy_count(),
            "unhealthy": self.unhealthy_count(),
            "avg_success_rate": self.avg_success_rate(),
        }


KeyFn = Callable[[PipelineMetric], Optional[str]]


def group_metrics(
    metrics: List[PipelineMetric],
    key_fn: KeyFn,
    default_key: str = "(ungrouped)",
) -> Dict[str, MetricGroup]:
    """Partition *metrics* into MetricGroup buckets using *key_fn*.

    Metrics for which *key_fn* returns ``None`` are placed in *default_key*.
    """
    groups: Dict[str, MetricGroup] = {}
    for metric in metrics:
        k = key_fn(metric) or default_key
        if k not in groups:
            groups[k] = MetricGroup(key=k)
        groups[k].metrics.append(metric)
    return groups


def group_by_pipeline(metrics: List[PipelineMetric]) -> Dict[str, MetricGroup]:
    """Convenience wrapper – group by pipeline name."""
    return group_metrics(metrics, lambda m: m.pipeline_name)


def group_by_tag_value(
    metrics: List[PipelineMetric], tag: str
) -> Dict[str, MetricGroup]:
    """Group metrics by the value of a specific tag."""
    def _key(m: PipelineMetric) -> Optional[str]:
        return (m.tags or {}).get(tag)

    return group_metrics(metrics, _key)
