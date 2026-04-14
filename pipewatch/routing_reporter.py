"""Human-readable and JSON reporters for alert routing results."""
from __future__ import annotations

import json
from typing import List

from pipewatch.routing import RoutingResult


def format_routing_result(result: RoutingResult) -> str:
    dest_str = ", ".join(result.destinations) if result.destinations else "(no destination)"
    status = "ROUTED" if result.routed else "UNROUTED"
    return (
        f"  [{status}] {result.alert.pipeline} / {result.alert.rule} "
        f"({result.alert.severity.value}) -> {dest_str}"
    )


def format_routing_report(results: List[RoutingResult]) -> str:
    if not results:
        return "Routing Report: no alerts to route."
    lines = ["Routing Report:", "="* 40]
    routed = sum(1 for r in results if r.routed)
    lines.append(f"  Alerts: {len(results)}  Routed: {routed}  Unrouted: {len(results) - routed}")
    lines.append("")
    for result in results:
        lines.append(format_routing_result(result))
    return "\n".join(lines)


def routing_report_to_json(results: List[RoutingResult]) -> str:
    return json.dumps(
        {
            "total": len(results),
            "routed": sum(1 for r in results if r.routed),
            "results": [r.to_dict() for r in results],
        },
        indent=2,
    )
