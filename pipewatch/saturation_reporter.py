"""Reporting helpers for saturation detection results."""
from __future__ import annotations
import json
from typing import List
from pipewatch.saturation import SaturationResult


def _icon(level: str) -> str:
    return {"ok": "\u2705", "warning": "\u26a0\ufe0f", "critical": "\U0001f6a8"}.get(level, "?")


def _bar(utilisation: float, width: int = 20) -> str:
    filled = round(utilisation * width)
    return "[" + "#" * filled + "-" * (width - filled) + "]"


def format_saturation_result(result: SaturationResult) -> str:
    if result.insufficient_data:
        return f"  {result.pipeline}: insufficient data"
    pct = result.utilisation * 100
    bar = _bar(result.utilisation)
    icon = _icon(result.level)
    return (
        f"  {icon} {result.pipeline}: {pct:.1f}% of ceiling "
        f"({result.avg_throughput:.1f} / {result.ceiling:.1f} rows/s) {bar}"
    )


def format_saturation_report(results: List[SaturationResult]) -> str:
    if not results:
        return "Saturation: no results."
    lines = ["=== Saturation Report ==="]
    for r in results:
        lines.append(format_saturation_result(r))
    return "\n".join(lines)


def saturation_report_to_json(results: List[SaturationResult]) -> str:
    return json.dumps([r.to_dict() for r in results], indent=2)
