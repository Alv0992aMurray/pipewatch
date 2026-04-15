"""Format cooldown results for CLI output and JSON export."""

from __future__ import annotations

import json
from typing import List

from pipewatch.cooldown import CooldownResult, CooldownEntry


def format_cooldown_result(result: CooldownResult) -> str:
    lines: List[str] = []
    lines.append(f"Cooldown filter: {len(result.kept)} kept, {result.total_suppressed} suppressed")
    if result.suppressed:
        lines.append("  Suppressed alerts:")
        for alert in result.suppressed:
            lines.append(f"    - [{alert.severity.value.upper()}] {alert.pipeline}: {alert.message}")
    return "\n".join(lines)


def format_cooldown_entries(entries: List[CooldownEntry]) -> str:
    if not entries:
        return "No active cooldowns."
    lines = ["Active cooldowns:"]
    for entry in entries:
        status = "cooling" if entry.is_cooling() else "ready"
        lines.append(
            f"  {entry.pipeline}: last alert {entry.last_alert_at.isoformat()} "
            f"(cooldown {entry.cooldown_seconds}s) [{status}]"
        )
    return "\n".join(lines)


def cooldown_report_to_json(result: CooldownResult, entries: List[CooldownEntry]) -> str:
    return json.dumps(
        {
            "kept": len(result.kept),
            "suppressed": result.total_suppressed,
            "suppressed_alerts": [
                {"pipeline": a.pipeline, "message": a.message, "severity": a.severity.value}
                for a in result.suppressed
            ],
            "entries": [e.to_dict() for e in entries],
        },
        indent=2,
    )
