"""Formatting helpers for incident reports."""

from __future__ import annotations

import json
from typing import List

from pipewatch.alerts import AlertSeverity
from pipewatch.incident import Incident


def _severity_icon(severity: AlertSeverity) -> str:
    return {AlertSeverity.CRITICAL: "🔴", AlertSeverity.WARNING: "🟡", AlertSeverity.INFO: "🔵"}.get(severity, "⚪")


def _status_label(incident: Incident) -> str:
    return "OPEN" if incident.is_open else "RESOLVED"


def format_incident(incident: Incident) -> str:
    icon = _severity_icon(incident.severity)
    status = _status_label(incident)
    duration = incident.duration_seconds or 0.0
    lines = [
        f"{icon} [{status}] {incident.title}",
        f"   ID       : {incident.incident_id}",
        f"   Pipeline : {incident.pipeline}",
        f"   Severity : {incident.severity.value.upper()}",
        f"   Alerts   : {len(incident.alerts)}",
        f"   Duration : {duration:.0f}s",
        f"   Opened   : {incident.opened_at.isoformat()}",
    ]
    if incident.resolved_at:
        lines.append(f"   Resolved : {incident.resolved_at.isoformat()}")
    return "\n".join(lines)


def format_incident_report(open_incidents: List[Incident], closed_incidents: List[Incident]) -> str:
    parts: List[str] = []
    if open_incidents:
        parts.append("=== Open Incidents ===")
        for inc in open_incidents:
            parts.append(format_incident(inc))
    else:
        parts.append("=== Open Incidents ===\n  (none)")

    if closed_incidents:
        parts.append("=== Resolved Incidents ===")
        for inc in closed_incidents:
            parts.append(format_incident(inc))

    return "\n\n".join(parts)


def incident_report_to_json(
    open_incidents: List[Incident], closed_incidents: List[Incident]
) -> str:
    return json.dumps(
        {
            "open": [i.to_dict() for i in open_incidents],
            "closed": [i.to_dict() for i in closed_incidents],
        },
        indent=2,
    )
