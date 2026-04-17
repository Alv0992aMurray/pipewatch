"""Formatting helpers for ReachabilityResult."""
from __future__ import annotations
import json
from pipewatch.reachability import ReachabilityResult


def format_reachability_result(result: ReachabilityResult) -> str:
    lines: list[str] = [
        f"Reachability from '{result.source}'",
        f"  Reachable ({result.total_reachable}): " +
        (", ".join(
            f"{n} (depth {result.depth_map[n]})" for n in result.reachable
        ) or "none"),
        f"  Unreachable ({len(result.unreachable)}): " +
        (", ".join(result.unreachable) or "none"),
    ]
    return "\n".join(lines)


def format_reachability_report(results: list[ReachabilityResult]) -> str:
    if not results:
        return "No reachability results."
    return "\n\n".join(format_reachability_result(r) for r in results)


def reachability_report_to_json(results: list[ReachabilityResult]) -> str:
    return json.dumps([r.to_dict() for r in results], indent=2)
