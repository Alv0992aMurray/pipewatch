"""Pipeline segmentation: group metrics into named segments based on tag criteria."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from pipewatch.metrics import PipelineMetric


@dataclass
class SegmentCriteria:
    tag_key: str
    tag_value: Optional[str] = None  # None means "any value"

    def matches(self, metric: PipelineMetric) -> bool:
        val = metric.tags.get(self.tag_key)
        if val is None:
            return False
        return self.tag_value is None or val == self.tag_value


@dataclass
class Segment:
    name: str
    criteria: SegmentCriteria
    metrics: List[PipelineMetric] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.metrics)

    @property
    def healthy_count(self) -> int:
        from pipewatch.metrics import is_healthy
        return sum(1 for m in self.metrics if is_healthy(m))

    @property
    def avg_success_rate(self) -> float:
        if not self.metrics:
            return 0.0
        from pipewatch.metrics import success_rate
        return sum(success_rate(m) for m in self.metrics) / len(self.metrics)

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "count": self.count,
            "healthy_count": self.healthy_count,
            "avg_success_rate": round(self.avg_success_rate, 4),
        }


@dataclass
class SegmentationResult:
    segments: List[Segment]
    unmatched: List[PipelineMetric] = field(default_factory=list)

    @property
    def total_metrics(self) -> int:
        return sum(s.count for s in self.segments) + len(self.unmatched)

    def get(self, name: str) -> Optional[Segment]:
        for s in self.segments:
            if s.name == name:
                return s
        return None


def segment_metrics(
    metrics: List[PipelineMetric],
    criteria_map: Dict[str, SegmentCriteria],
) -> SegmentationResult:
    """Assign each metric to the first matching segment; unmatched go to remainder."""
    segments = [Segment(name=name, criteria=c) for name, c in criteria_map.items()]
    unmatched: List[PipelineMetric] = []
    for metric in metrics:
        placed = False
        for seg in segments:
            if seg.criteria.matches(metric):
                seg.metrics.append(metric)
                placed = True
                break
        if not placed:
            unmatched.append(metric)
    return SegmentationResult(segments=segments, unmatched=unmatched)
