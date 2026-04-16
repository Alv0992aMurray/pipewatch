"""Formatting helpers for recurrence detection results."""
from __future__ import annotations

import json
from typing import List

from pipewatch.recurrence import RecurrenceEntry, RecurrenceResult


def format_recurrence_entry(entry: RecurrenceEntry) -> str:
    tag = " [RECURRING]" if entry.occurrences > 1 else ""
    return (
        f"  {entry.pipeline} / {entry.rule_name}{tag}\n"
        f"    occurrences : {entry.occurrences}\n"
        f"    first seen  : {entry.first_seen.isoformat()}\n"
        f"    last seen   : {entry.last_seen.isoformat()}"
    )


def format_recurrence_report(result: RecurrenceResult) -> str:
    lines: List[str] = ["=== Recurrence Report ==="]
    if not result.entries:
        lines.append("  No recurrence data recorded.")
        return "\n".join(lines)

    lines.append(f"  Total recurring alerts : {result.total_recurring}")
    for entry in sorted(result.entries, key=lambda e: e.occurrences, reverse=True):
        lines.append(format_recurrence_entry(entry))
    return "\n".join(lines)


def recurrence_report_to_json(result: RecurrenceResult) -> str:
    return json.dumps(result.to_dict(), indent=2)
