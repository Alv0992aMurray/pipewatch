"""Format escalation results for CLI output and JSON export."""
from __future__ import annotations

import json
from typing import List

from pipewatch.escalation import EscalationEntry, EscalationResult


def format_escalation_result(result: EscalationResult) -> str:
    lines: List[str] = []
    if not result.alerts:
        lines.append("  No active alerts.")
        return "\n".join(lines)

    for alert in result.alerts:
        tag = " [ESCALATED]" if alert in result.escalated else ""
        lines.append(
            f"  [{alert.severity.name}]{tag} {alert.pipeline} / {alert.rule_name}: {alert.message}"
        )
    return "\n".join(lines)


def format_escalation_entries(entries: List[EscalationEntry]) -> str:
    if not entries:
        return "  No escalation tracking data."
    lines: List[str] = []
    for e in entries:
        fired = e.last_fired.isoformat() if e.last_fired else "never"
        lines.append(
            f"  {e.pipeline} / {e.rule_name}: {e.count} firing(s), last={fired}"
        )
    return "\n".join(lines)


def format_escalation_report(
    result: EscalationResult,
    entries: List[EscalationEntry],
) -> str:
    parts = [
        "=== Escalation Report ===",
        f"Total alerts : {len(result.alerts)}",
        f"Escalated    : {result.total_escalated}",
        "",
        "Alerts:",
        format_escalation_result(result),
        "",
        "Firing counts:",
        format_escalation_entries(entries),
    ]
    return "\n".join(parts)


def escalation_report_to_json(
    result: EscalationResult,
    entries: List[EscalationEntry],
) -> str:
    payload = {
        "total_alerts": len(result.alerts),
        "total_escalated": result.total_escalated,
        "alerts": [
            {
                "pipeline": a.pipeline,
                "rule_name": a.rule_name,
                "severity": a.severity.name,
                "message": a.message,
                "escalated": a in result.escalated,
            }
            for a in result.alerts
        ],
        "entries": [e.to_dict() for e in entries],
    }
    return json.dumps(payload, indent=2)
