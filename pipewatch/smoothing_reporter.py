"""Formatting helpers for SmoothingResult."""
from __future__ import annotations

import json
from typing import List

from pipewatch.smoothing import SmoothingResult


def _flag(result: SmoothingResult) -> str:
    if result.insufficient_data:
        return "⚠"
    latest = result.latest_smoothed()
    if latest is None:
        return "?"
    return "✔" if latest >= 0.95 else "✘"


def format_smoothing_result(result: SmoothingResult) -> str:
    """Return a single-line summary of a smoothing result."""
    flag = _flag(result)
    if result.insufficient_data:
        return (
            f"{flag}  [{result.pipeline}] {result.metric}: insufficient data "
            f"(alpha={result.alpha})"
        )
    latest = result.latest_smoothed()
    n = len(result.points)
    return (
        f"{flag}  [{result.pipeline}] {result.metric}: "
        f"EMA={latest:.4f}  n={n}  alpha={result.alpha}"
    )


def format_smoothing_report(results: List[SmoothingResult]) -> str:
    """Return a multi-line report for a list of smoothing results."""
    if not results:
        return "No smoothing results."
    lines = ["=== Smoothing Report ==="]
    for r in results:
        lines.append(format_smoothing_result(r))
    return "\n".join(lines)


def smoothing_report_to_json(results: List[SmoothingResult]) -> str:
    return json.dumps([r.to_dict() for r in results], indent=2)
