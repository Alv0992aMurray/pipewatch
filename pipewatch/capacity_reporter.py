"""Human-readable and JSON reporters for capacity results."""
from __future__ import annotations

import json
from typing import List

from pipewatch.capacity import CapacityResult


def _fmt(v: float | None, decimals: int = 4) -> str:
    return f"{v:.{decimals}f}" if v is not None else "n/a"


def format_capacity_result(result: CapacityResult) -> str:
    lines = [
        f"Pipeline : {result.pipeline}",
        f"Metric   : {result.metric}",
        f"Current  : {_fmt(result.current_value, 4)}",
        f"Threshold: {_fmt(result.threshold, 4)}  ({result.direction})",
        f"Slope/run: {_fmt(result.slope_per_run, 6)}",
    ]
    if result.will_breach:
        lines.append(f"⚠  Breach estimated in ~{result.runs_until_breach} run(s)")
    else:
        lines.append("✓  No breach projected")
    return "\n".join(lines)


def format_capacity_report(results: List[CapacityResult]) -> str:
    if not results:
        return "No capacity estimates available."
    sections = []
    for r in results:
        sections.append(format_capacity_result(r))
    return "\n\n".join(sections)


def capacity_report_to_json(results: List[CapacityResult]) -> str:
    return json.dumps([r.to_dict() for r in results], indent=2)
