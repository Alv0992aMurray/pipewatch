"""Reporter for aggregation groups."""
from __future__ import annotations

import json
from typing import List

from pipewatch.aggregation import AggregationGroup


def _fmt_pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.1f}%"


def _fmt_float(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.2f}"


def format_group(group: AggregationGroup) -> str:
    lines = [
        f"Group : {group.name}",
        f"  Pipelines : {group.count} total, {group.healthy_count} healthy, {group.unhealthy_count} unhealthy",
        f"  Health    : {_fmt_pct(group.health_ratio)}",
        f"  Avg SR    : {_fmt_pct(group.avg_success_rate)}",
        f"  Avg TP    : {_fmt_float(group.avg_throughput)} rows/s",
    ]
    return "\n".join(lines)


def format_aggregation_report(groups: List[AggregationGroup]) -> str:
    if not groups:
        return "No aggregation groups."
    sections = [format_group(g) for g in groups]
    return "\n\n".join(sections)


def aggregation_report_to_json(groups: List[AggregationGroup]) -> str:
    return json.dumps([g.to_dict() for g in groups], indent=2)
