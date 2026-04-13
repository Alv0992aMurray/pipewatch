"""Tag-based filtering for pipeline metrics and rules."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

from pipewatch.metrics import PipelineMetric


@dataclass
class TagFilter:
    """A filter that matches metrics by tag key/value pairs."""

    required: dict[str, str] = field(default_factory=dict)

    def matches(self, metric: PipelineMetric) -> bool:
        """Return True if *metric* carries all required tags with correct values."""
        tags: dict[str, str] = getattr(metric, "tags", {}) or {}
        for key, value in self.required.items():
            if tags.get(key) != value:
                return False
        return True


def filter_metrics(
    metrics: Iterable[PipelineMetric],
    tag_filter: TagFilter,
) -> list[PipelineMetric]:
    """Return only the metrics that satisfy *tag_filter*."""
    return [m for m in metrics if tag_filter.matches(m)]


def group_by_tag(
    metrics: Iterable[PipelineMetric],
    tag_key: str,
) -> dict[str, list[PipelineMetric]]:
    """Group *metrics* by the value of *tag_key*.

    Metrics that do not carry the tag are placed under the empty-string key.
    """
    groups: dict[str, list[PipelineMetric]] = {}
    for metric in metrics:
        tags: dict[str, str] = getattr(metric, "tags", {}) or {}
        bucket = tags.get(tag_key, "")
        groups.setdefault(bucket, []).append(metric)
    return groups
