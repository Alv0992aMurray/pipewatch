"""Reporting helpers for pattern detection results."""
from __future__ import annotations

import json
from typing import List

from pipewatch.pattern import PatternResult

_ICONS = {
    "stable_healthy": "✅",
    "occasional_failure": "⚠️",
    "flapping": "🔀",
    "degraded": "🔴",
    "sustained_failure": "🚨",
}


def format_pattern_result(result: PatternResult) -> str:
    icon = _ICONS.get(result.pattern_label, "❓")
    lines = [
        f"{icon} [{result.pipeline}] pattern: {result.pattern_label}",
        f"   metric          : {result.metric}",
        f"   total snapshots : {result.total_snapshots}",
        f"   failure runs    : {result.failure_runs}",
        f"   consecutive fail: {result.consecutive_failures}",
        f"   flapping        : {'yes' if result.alternating else 'no'}",
    ]
    if result.note:
        lines.append(f"   note            : {result.note}")
    return "\n".join(lines)


def format_pattern_report(results: List[PatternResult]) -> str:
    if not results:
        return "No pattern data available."
    sections = [format_pattern_result(r) for r in results]
    header = f"Pattern Report ({len(results)} pipeline(s))"
    separator = "-" * 44
    return "\n".join([header, separator] + sections)


def pattern_report_to_json(results: List[PatternResult]) -> str:
    return json.dumps([r.to_dict() for r in results], indent=2)
