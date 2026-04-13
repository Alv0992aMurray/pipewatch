"""Format ReplayResult objects for CLI output and JSON export."""

from __future__ import annotations

import json
from typing import List

from pipewatch.replay import ReplayResult, ReplayEvent
from pipewatch.alerts import AlertSeverity


_SEVERITY_COLOUR = {
    AlertSeverity.CRITICAL: "\u274c",
    AlertSeverity.WARNING: "\u26a0\ufe0f",
    AlertSeverity.INFO: "\u2139\ufe0f",
}


def _format_event(event: ReplayEvent) -> str:
    ts = event.snapshot.timestamp.strftime("%Y-%m-%d %H:%M:%S")
    if not event.has_alerts():
        return f"  [{ts}] \u2705 no alerts"
    lines = [f"  [{ts}] {len(event.alerts)} alert(s):"]
    for alert in event.alerts:
        icon = _SEVERITY_COLOUR.get(alert.severity, "\u2022")
        lines.append(f"    {icon} [{alert.severity.value.upper()}] {alert.rule_name}: {alert.message}")
    return "\n".join(lines)


def format_replay_result(result: ReplayResult) -> str:
    """Return a human-readable replay report string."""
    header = (
        f"Replay: {result.pipeline_name} "
        f"| snapshots={result.total_snapshots} "
        f"| alert events={result.total_alert_events} "
        f"| total alerts={result.total_alerts}"
    )
    lines = [header, "-" * len(header)]
    if not result.events:
        lines.append("  (no snapshots to replay)")
    else:
        for event in result.events:
            lines.append(_format_event(event))
    return "\n".join(lines)


def replay_to_json(result: ReplayResult) -> str:
    """Serialise a ReplayResult to a JSON string."""
    payload = {
        "pipeline": result.pipeline_name,
        "total_snapshots": result.total_snapshots,
        "total_alert_events": result.total_alert_events,
        "total_alerts": result.total_alerts,
        "events": [e.to_dict() for e in result.events],
    }
    return json.dumps(payload, indent=2)
