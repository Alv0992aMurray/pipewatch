"""Human-readable and JSON formatting for NotificationResult."""
from __future__ import annotations

import json
from typing import List

from pipewatch.notification import NotificationResult


def format_notification_result(result: NotificationResult) -> str:
    """Return a multi-line summary of which channels received which alerts."""
    if result.total_routed() == 0:
        return "No alerts routed to any channel."

    lines: List[str] = []
    for channel, alerts in result.routed.items():
        if not alerts:
            lines.append(f"  [{channel}] — no alerts")
        else:
            lines.append(f"  [{channel}] {len(alerts)} alert(s):")
            for alert in alerts:
                sev = alert.severity.name
                lines.append(f"    • [{sev}] {alert.pipeline}: {alert.message}")
    return "Notification routing:\n" + "\n".join(lines)


def format_notification_report(results: List[NotificationResult]) -> str:
    """Format a list of routing results (one per pipeline run)."""
    if not results:
        return "No notification results."
    sections = [format_notification_result(r) for r in results]
    return "\n\n".join(sections)


def notification_report_to_json(result: NotificationResult) -> str:
    """Serialise a NotificationResult to a JSON string."""
    return json.dumps(result.to_dict(), indent=2)
