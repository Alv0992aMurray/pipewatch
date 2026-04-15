"""Human-readable and JSON reporters for MetricGroup results."""
from __future__ import annotations

import json
from typing import Dict, List

from pipewatch.grouping import MetricGroup


def _fmt_pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.1f}%"


def format_group(group: MetricGroup) -> str:
    """Return a single-line summary for one MetricGroup."""
    rate_str = _fmt_pct(group.avg_success_rate())
    health = "OK" if group.unhealthy_count() == 0 else "DEGRADED"
    return (
        f"[{health}] {group.key} — "
        f"{group.count()} pipeline(s), "
        f"{group.healthy_count()} healthy, "
        f"{group.unhealthy_count()} unhealthy, "
        f"avg success rate: {rate_str}"
    )


def format_grouping_report(groups: Dict[str, MetricGroup]) -> str:
    """Return a multi-line report covering all groups."""
    if not groups:
        return "No groups to report."
    lines = ["=== Pipeline Grouping Report ==="]
    for key in sorted(groups):
        lines.append(format_group(groups[key]))
    return "\n".join(lines)


def grouping_report_to_json(
    groups: Dict[str, MetricGroup],
) -> str:
    """Serialise all groups to a JSON string."""
    payload: List[dict] = [
        groups[k].to_dict() for k in sorted(groups)
    ]
    return json.dumps(payload, indent=2)
