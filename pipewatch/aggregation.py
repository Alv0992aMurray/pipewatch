"""Aggregation: combine metrics from multiple pipelines into group-level stats."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class AggregationGroup:
    """A named group of pipeline metrics with computed aggregate statistics."""

    name: str
    metrics: List[PipelineMetric] = field(default_factory=list)

    # --- derived properties ---------------------------------------------------

    @property
    def count(self) -> int:
        return len(self.metrics)

    @property
    def healthy_count(self) -> int:
        from pipewatch.metrics import is_healthy
        return sum(1 for m in self.metrics if is_healthy(m))

    @property
    def unhealthy_count(self) -> int:
        return self.count - self.healthy_count

    @property
    def avg_success_rate(self) -> Optional[float]:
        from pipewatch.metrics import success_rate
        rates = [r for m in self.metrics if (r := success_rate(m)) is not None]
        if not rates:
            return None
        return sum(rates) / len(rates)

    @property
    def avg_throughput(self) -> Optional[float]:
        from pipewatch.metrics import throughput
        tps = [t for m in self.metrics if (t := throughput(m)) is not None]
        if not tps:
            return None
        return sum(tps) / len(tps)

    @property
    def health_ratio(self) -> Optional[float]:
        if self.count == 0:
            return None
        return self.healthy_count / self.count

    def to_dict(self) -> Dict:
        return {
            "group": self.name,
            "count": self.count,
            "healthy": self.healthy_count,
            "unhealthy": self.unhealthy_count,
            "health_ratio": round(self.health_ratio, 4) if self.health_ratio is not None else None,
            "avg_success_rate": round(self.avg_success_rate, 4) if self.avg_success_rate is not None else None,
            "avg_throughput": round(self.avg_throughput, 4) if self.avg_throughput is not None else None,
        }


def aggregate_by_tag(metrics: List[PipelineMetric], tag_key: str) -> List[AggregationGroup]:
    """Group metrics by a tag value and return one AggregationGroup per distinct value."""
    groups: Dict[str, AggregationGroup] = {}
    for m in metrics:
        tag_value = (m.tags or {}).get(tag_key, "__untagged__")
        if tag_value not in groups:
            groups[tag_value] = AggregationGroup(name=tag_value)
        groups[tag_value].metrics.append(m)
    return list(groups.values())
