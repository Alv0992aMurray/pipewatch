"""Formatters for correlation analysis results."""

from __future__ import annotations

import json
from typing import List

from pipewatch.correlation import CorrelationResult


def _strength_label(strength: str) -> str:
    labels = {
        "strong": "\u25cf Strong",
        "moderate": "\u25d1 Moderate",
        "weak": "\u25cb Weak",
        "none": "  None",
    }
    return labels.get(strength, strength)


def _direction_label(direction: str) -> str:
    if direction == "positive":
        return "(+)"
    if direction == "negative":
        return "(-)"
    return "   "


def format_correlation_result(result: CorrelationResult) -> str:
    """Return a single-line summary of a CorrelationResult."""
    if result.r is None:
        return (
            f"  {result.pipeline_a} <-> {result.pipeline_b}: "
            "insufficient data"
        )
    strength = _strength_label(result.strength)
    direction = _direction_label(result.direction)
    return (
        f"  {result.pipeline_a} <-> {result.pipeline_b}: "
        f"{strength} {direction}  r={result.r:+.3f}  "
        f"[{result.interpretation}]"
    )


def format_correlation_report(results: List[CorrelationResult]) -> str:
    """Return a formatted multi-line correlation report."""
    if not results:
        return "Correlation Report\n  (no pairs analysed)"

    lines = ["Correlation Report"]
    for result in results:
        lines.append(format_correlation_result(result))
    return "\n".join(lines)


def correlation_report_to_json(results: List[CorrelationResult]) -> str:
    """Serialise a list of CorrelationResult objects to a JSON string."""
    return json.dumps([r.to_dict() for r in results], indent=2)
