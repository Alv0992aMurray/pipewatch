"""Human-readable and JSON reporting for TaperingResult."""
from __future__ import annotations

import json
from typing import List

from pipewatch.tapering import TaperingResult


def _flag(result: TaperingResult) -> str:
    if result.insufficient_data:
        return "⏳"
    return "📉" if result.is_tapering else "✅"


def format_tapering_result(result: TaperingResult) -> str:
    icon = _flag(result)
    lines = [f"{icon} [{result.pipeline}] {result.metric}"]

    if result.insufficient_data:
        lines.append("  Insufficient data to determine tapering trend.")
        return "\n".join(lines)

    slope_str = f"{result.slope:+.4f}" if result.slope is not None else "n/a"
    current_str = f"{result.current_value:.4f}" if result.current_value is not None else "n/a"
    lines.append(f"  Current: {current_str}  Threshold: {result.threshold:.4f}  Slope: {slope_str}/snapshot")

    if result.is_tapering:
        if result.projected_breach is not None:
            lines.append(
                f"  ⚠ Tapering detected — projected breach in ~{result.projected_breach} snapshot(s)."
            )
        else:
            lines.append("  ⚠ Tapering detected.")
    else:
        lines.append("  No tapering detected.")

    return "\n".join(lines)


def format_tapering_report(results: List[TaperingResult]) -> str:
    if not results:
        return "No tapering results to display."
    return "\n\n".join(format_tapering_result(r) for r in results)


def tapering_report_to_json(results: List[TaperingResult]) -> str:
    return json.dumps([r.to_dict() for r in results], indent=2)
