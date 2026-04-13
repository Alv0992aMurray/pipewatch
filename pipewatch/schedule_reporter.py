"""Formatting helpers for schedule check results."""

from __future__ import annotations

import json
from typing import List

from pipewatch.schedule import ScheduleCheck, ScheduleStatus

_STATUS_ICONS = {
    ScheduleStatus.ON_TIME: "✅",
    ScheduleStatus.LATE: "⚠️",
    ScheduleStatus.MISSING: "🔴",
    ScheduleStatus.UNKNOWN: "❓",
}


def format_schedule_check(check: ScheduleCheck) -> str:
    """Return a single-line human-readable summary of a schedule check."""
    icon = _STATUS_ICONS.get(check.status, "?")
    last_run_str = check.last_run.strftime("%Y-%m-%d %H:%M:%S") if check.last_run else "never"
    expected_str = (
        check.expected_by.strftime("%Y-%m-%d %H:%M:%S") if check.expected_by else "unknown"
    )
    return (
        f"{icon} [{check.status.value.upper()}] {check.pipeline_name} "
        f"| last run: {last_run_str} | expected by: {expected_str}"
    )


def format_schedule_report(checks: List[ScheduleCheck]) -> str:
    """Return a multi-line report for a list of schedule checks."""
    if not checks:
        return "No schedule checks to report."

    lines = ["=== Schedule Report ==="]
    for check in checks:
        lines.append(format_schedule_check(check))

    overdue = [c for c in checks if c.is_overdue()]
    lines.append("")
    lines.append(f"Pipelines checked : {len(checks)}")
    lines.append(f"Overdue           : {len(overdue)}")
    return "\n".join(lines)


def schedule_report_to_json(checks: List[ScheduleCheck]) -> str:
    """Serialise schedule checks to a JSON string."""
    return json.dumps([c.to_dict() for c in checks], indent=2)
