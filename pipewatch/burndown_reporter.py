"""Reporters for burndown results."""
from __future__ import annotations

import json
from typing import List

from pipewatch.burndown import BurndownResult

_TREND_ICON = {
    "improving": "↓",
    "worsening": "↑",
    "stable": "→",
    "insufficient_data": "?",
}


def format_burndown_result(result: BurndownResult) -> str:
    icon = _TREND_ICON.get(result.trend, "?")
    rate = result.resolution_rate
    rate_str = f"{rate:.1%}" if rate is not None else "n/a"
    lines = [
        f"[Burndown] {result.pipeline}",
        f"  Trend       : {icon} {result.trend}",
        f"  Opened      : {result.total_opened}",
        f"  Resolved    : {result.total_resolved}",
        f"  Resolution  : {rate_str}",
    ]
    if result.points:
        lines.append("  Points:")
        for p in result.points:
            lines.append(
                f"    {p.timestamp.strftime('%Y-%m-%dT%H:%M')}  "
                f"open={p.open_count}  resolved={p.resolved_count}"
            )
    return "\n".join(lines)


def format_burndown_report(results: List[BurndownResult]) -> str:
    if not results:
        return "No burndown data."
    return "\n\n".join(format_burndown_result(r) for r in results)


def burndown_report_to_json(results: List[BurndownResult]) -> str:
    return json.dumps([r.to_dict() for r in results], indent=2)
