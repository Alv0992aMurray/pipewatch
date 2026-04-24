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
    """Format a single SaturationResult as a human-readable string.

    Returns a short message for pipelines with insufficient data, or a
    detailed line showing utilisation percentage, throughput vs ceiling,
    and a visual bar for pipelines with enough data.
    """
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
    """Format a list of SaturationResults as a multi-line report string."""
    if not results:
        return "Saturation: no results."
    lines = ["=== Saturation Report ==="]
    for r in results:
        lines.append(format_saturation_result(r))
    return "\n".join(lines)


def saturation_report_to_json(results: List[SaturationResult]) -> str:
    """Serialise a list of SaturationResults to a JSON string."""
    return json.dumps([r.to_dict() for r in results], indent=2)


def summarise_saturation_report(results: List[SaturationResult]) -> str:
    """Return a one-line summary of saturation levels across all pipelines.

    Counts pipelines in each level (ok, warning, critical) and those with
    insufficient data, then returns a compact summary string.
    """
    counts: dict[str, int] = {"ok": 0, "warning": 0, "critical": 0, "insufficient": 0}
    for r in results:
        if r.insufficient_data:
            counts["insufficient"] += 1
        else:
            counts[r.level] = counts.get(r.level, 0) + 1
    total = len(results)
    return (
        f"Saturation summary ({total} pipeline(s)): "
        f"{_icon('ok')} {counts['ok']} ok, "
        f"{_icon('warning')} {counts['warning']} warning, "
        f"{_icon('critical')} {counts['critical']} critical, "
        f"? {counts['insufficient']} insufficient data"
    )
