"""Human-readable and JSON reporting for silence results."""
from __future__ import annotations

import json
from typing import List

from pipewatch.silencing import SilenceResult, SilenceWindow


def format_silence_result(result: SilenceResult) -> str:
    lines: List[str] = []
    if not result.kept and not result.silenced:
        lines.append("  (no alerts)")
        return "\n".join(lines)
    for alert in result.kept:
        lines.append(f"  [KEPT]     {alert.pipeline} — {alert.message}")
    for alert in result.silenced:
        lines.append(f"  [SILENCED] {alert.pipeline} — {alert.message}")
    return "\n".join(lines)


def format_silence_windows(windows: List[SilenceWindow]) -> str:
    if not windows:
        return "  (no silence windows configured)"
    lines: List[str] = []
    for w in windows:
        status = "ACTIVE" if w.is_active() else "inactive"
        lines.append(
            f"  [{status}] {w.name} | pipeline={w.pipeline} "
            f"{w.start.isoformat()} → {w.end.isoformat()}"
            + (f" ({w.reason})" if w.reason else "")
        )
    return "\n".join(lines)


def silencing_report_to_json(result: SilenceResult) -> str:
    payload = {
        "kept": [a.to_dict() if hasattr(a, "to_dict") else str(a) for a in result.kept],
        "silenced": [a.to_dict() if hasattr(a, "to_dict") else str(a) for a in result.silenced],
        "total_silenced": result.total_silenced,
    }
    return json.dumps(payload, indent=2)
