"""Formatting helpers for dependency check results."""
from __future__ import annotations

import json
from typing import List

from pipewatch.dependency import DependencyResult


def _status_icon(result: DependencyResult) -> str:
    return "\u274c" if result.is_blocked else "\u2705"


def format_dependency_result(result: DependencyResult) -> str:
    """Format a single DependencyResult into a human-readable string."""
    icon = _status_icon(result)
    lines = [f"{icon}  {result.message}"]
    if result.blocked_by:
        lines.append(f"   Blocked by: {', '.join(result.blocked_by)}")
    if result.upstream_pipelines:
        lines.append(f"   Upstreams : {', '.join(result.upstream_pipelines)}")
    return "\n".join(lines)


def format_dependency_report(results: List[DependencyResult]) -> str:
    """Format a list of DependencyResults into a full report string."""
    if not results:
        return "No dependency relationships configured."

    blocked = [r for r in results if r.is_blocked]
    header_parts = [f"Dependency Report — {len(results)} pipeline(s) checked"]
    if blocked:
        header_parts.append(f"{len(blocked)} blocked")
    header = ", ".join(header_parts)

    sections = [header, "-" * len(header)]
    for result in results:
        sections.append(format_dependency_result(result))
    return "\n".join(sections)


def dependency_report_to_json(results: List[DependencyResult]) -> str:
    """Serialize a list of DependencyResults to a JSON string."""
    return json.dumps([r.to_dict() for r in results], indent=2)


def summarize_dependency_report(results: List[DependencyResult]) -> str:
    """Return a one-line summary of the dependency check results.

    Example output:
        "3 pipeline(s) checked: 1 blocked, 2 passing"
    """
    total = len(results)
    blocked = sum(1 for r in results if r.is_blocked)
    passing = total - blocked
    return f"{total} pipeline(s) checked: {blocked} blocked, {passing} passing"
