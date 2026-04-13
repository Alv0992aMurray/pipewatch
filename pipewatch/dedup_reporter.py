"""Human-readable and JSON reporting for deduplication results."""
from __future__ import annotations

import json
from typing import List

from pipewatch.dedup import DedupResult, DedupEntry


def format_dedup_result(result: DedupResult) -> str:
    lines: List[str] = []
    if not result.kept and not result.suppressed:
        lines.append("  (no alerts to deduplicate)")
        return "\n".join(lines)

    if result.kept:
        lines.append(f"  Forwarded ({len(result.kept)}):")
        for alert in result.kept:
            lines.append(f"    [{alert.severity.value.upper()}] {alert.pipeline} — {alert.message}")

    if result.suppressed:
        lines.append(f"  Suppressed as duplicate ({result.total_suppressed}):")
        for alert in result.suppressed:
            lines.append(f"    [{alert.severity.value.upper()}] {alert.pipeline} — {alert.message}")

    return "\n".join(lines)


def format_dedup_entries(entries: List[DedupEntry]) -> str:
    if not entries:
        return "  (no dedup history)"
    lines = ["  Dedup cache:"]
    for e in entries:
        lines.append(
            f"    {e.pipeline}::{e.rule_name}  fired={e.count}  "
            f"first={e.first_seen:.0f}  last={e.last_seen:.0f}"
        )
    return "\n".join(lines)


def dedup_report_to_json(result: DedupResult) -> str:
    payload = {
        "kept": [a.to_dict() for a in result.kept],
        "suppressed": [a.to_dict() for a in result.suppressed],
        "total_suppressed": result.total_suppressed,
    }
    return json.dumps(payload, indent=2)
