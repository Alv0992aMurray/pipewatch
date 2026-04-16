"""Reporting helpers for alert inhibition results."""
from __future__ import annotations
import json
from pipewatch.inhibition import InhibitionResult


def format_inhibition_result(result: InhibitionResult) -> str:
    lines = []
    lines.append(f"Inhibition summary: {result.total_inhibited} suppressed, {len(result.kept)} kept")
    if result.inhibited:
        lines.append("  Inhibited alerts:")
        for alert in result.inhibited:
            lines.append(f"    - [{alert.severity.value.upper()}] {alert.pipeline}: {alert.message}")
    if result.kept:
        lines.append("  Active alerts:")
        for alert in result.kept:
            lines.append(f"    - [{alert.severity.value.upper()}] {alert.pipeline}: {alert.message}")
    return "\n".join(lines)


def inhibition_report_to_json(result: InhibitionResult) -> str:
    return json.dumps(result.to_dict(), indent=2)
