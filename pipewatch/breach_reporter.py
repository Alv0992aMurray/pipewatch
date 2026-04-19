"""Reporting utilities for threshold breach results."""
from __future__ import annotations
import json
from typing import List
from pipewatch.breach import BreachResult


def _flag(result: BreachResult) -> str:
    return "🔴" if result.total_breaches > 0 else "✅"


def format_breach_result(result: BreachResult) -> str:
    flag = _flag(result)
    lines = [
        f"{flag} [{result.pipeline}] {result.config.metric} {result.config.direction} "
        f"{result.config.threshold} — {result.total_breaches} breach(es)"
    ]
    if result.latest:
        ev = result.latest
        lines.append(
            f"   latest: {ev.value:.4f} at {ev.timestamp.strftime('%Y-%m-%d %H:%M:%S')}"
        )
    return "\n".join(lines)


def format_breach_report(results: List[BreachResult]) -> str:
    if not results:
        return "No breach checks configured."
    return "\n".join(format_breach_result(r) for r in results)


def breach_report_to_json(results: List[BreachResult]) -> str:
    return json.dumps([r.to_dict() for r in results], indent=2)
