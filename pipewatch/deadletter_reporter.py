"""Reporter for dead-letter queue entries."""
from __future__ import annotations

import json
from typing import List

from pipewatch.deadletter import DeadLetterEntry, DeadLetterQueue


def format_entry(entry: DeadLetterEntry) -> str:
    ts = entry.last_failed_at.strftime("%Y-%m-%d %H:%M:%S")
    return (
        f"  ✖ [{entry.alert.severity.value.upper()}] {entry.alert.pipeline} "
        f"/ {entry.alert.rule_name} — {entry.reason} "
        f"(attempts: {entry.attempts}, last: {ts})"
    )


def format_deadletter_report(queue: DeadLetterQueue) -> str:
    entries = queue.all()
    if not entries:
        return "Dead-letter queue: empty"
    lines = [f"Dead-letter queue: {len(entries)} undelivered alert(s)"]
    for entry in entries:
        lines.append(format_entry(entry))
    return "\n".join(lines)


def deadletter_report_to_json(queue: DeadLetterQueue) -> str:
    return json.dumps(
        {"dead_letter_count": queue.count(), "entries": [e.to_dict() for e in queue.all()]},
        indent=2,
    )
