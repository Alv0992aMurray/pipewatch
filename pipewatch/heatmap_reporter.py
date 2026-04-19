"""Reporters for heatmap results."""
from __future__ import annotations
import json
from typing import List
from pipewatch.heatmap import HeatmapResult


def _bar(ratio: float, width: int = 10) -> str:
    filled = round(ratio * width)
    return "█" * filled + "░" * (width - filled)


def format_heatmap_result(result: HeatmapResult) -> str:
    lines = [f"Heatmap: {result.pipeline}"]
    if not result.cells:
        lines.append("  No data available.")
        return "\n".join(lines)

    lines.append(f"  {'Hour':>4}  {'Samples':>7}  {'Avg Success':>11}  Chart")
    lines.append("  " + "-" * 44)
    for cell in result.cells:
        bar = _bar(cell.avg_success_rate)
        lines.append(
            f"  {cell.hour:>4}  {cell.sample_count:>7}  "
            f"{cell.avg_success_rate * 100:>10.1f}%  {bar}"
        )

    worst = result.worst_hour()
    best = result.best_hour()
    lines.append(f"  Best hour: {best:02d}:00   Worst hour: {worst:02d}:00")
    return "\n".join(lines)


def format_heatmap_report(results: List[HeatmapResult]) -> str:
    if not results:
        return "No heatmap data."
    return "\n\n".join(format_heatmap_result(r) for r in results)


def heatmap_report_to_json(results: List[HeatmapResult]) -> str:
    return json.dumps([r.to_dict() for r in results], indent=2)
