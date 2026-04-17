"""Format maintenance window results for CLI output."""
from __future__ import annotations

import json
from typing import List

from pipewatch.maintenance import MaintenanceResult, MaintenanceWindow


def format_maintenance_result(result: MaintenanceResult) -> str:
    lines = []
    if result.suppressed:
        lines.append(f"  Suppressed during maintenance ({len(result.suppressed)}):")
        for a in result.suppressed:
            lines.append(f"    - [{a.severity.value.upper()}] {a.pipeline}: {a.message}")
    if result.kept:
        lines.append(f"  Active alerts ({len(result.kept)}):")
        for a in result.kept:
            lines.append(f"    - [{a.severity.value.upper()}] {a.pipeline}: {a.message}")
    if not lines:
        lines.append("  No alerts.")
    return "\n".join(lines)


def format_maintenance_windows(windows: List[MaintenanceWindow]) -> str:
    if not windows:
        return "  No maintenance windows configured."
    lines = []
    for w in windows:
        status = "ACTIVE" if w.is_active() else "inactive"
        reason = f" — {w.reason}" if w.reason else ""
        lines.append(f"  [{status}] {w.pipeline}: {w.start.isoformat()} → {w.end.isoformat()}{reason}")
    return "\n".join(lines)


def maintenance_report_to_json(result: MaintenanceResult) -> str:
    return json.dumps({
        "kept": [a.message for a in result.kept],
        "suppressed": [a.message for a in result.suppressed],
        "total_suppressed": result.total_suppressed,
    })
