"""Formatting helpers for dependency check results."""
from __future__ import annotations

import json
from typing import List

from pipewatch.dependency import DependencyResult


def _status_icon(result: DependencyResult) -> str:
    return "\u274c" if result.is_blocked else "\u2705"


def format_dependency_result(result: DependencyResult) -> str:
    icon = _status_icon(result)
    lines = [f"{icon}  {result.message}"]
    if result.blocked_by:
        lines.append(f"   Blocked by: {', '.join(result.blocked_by)}")
    if result.upstream_pipelines:
        lines.append(f"   Upstreams : {', '.join(result.upstream_pipelines)}")
    return "\n".join(lines)


def format_dependency_report(results: List[DependencyResult]) -> str:
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
    return json.dumps([r.to_dict() for r in results], indent=2)
