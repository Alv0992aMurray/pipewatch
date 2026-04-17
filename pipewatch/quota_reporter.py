"""Formatting helpers for quota results."""
from __future__ import annotations

import json
from pipewatch.quota import QuotaResult


def format_quota_result(result: QuotaResult, pipeline: str) -> str:
    lines = [f"Quota report — {pipeline}"]
    lines.append(f"  Kept    : {len(result.kept)}")
    lines.append(f"  Dropped : {result.total_dropped}")
    if result.dropped:
        lines.append("  Dropped alerts:")
        for alert in result.dropped:
            lines.append(f"    - [{alert.severity.value.upper()}] {alert.rule_name}")
    return "\n".join(lines)


def format_quota_report(results: list[tuple[str, QuotaResult]]) -> str:
    if not results:
        return "No quota data."
    return "\n\n".join(format_quota_result(r, p) for p, r in results)


def quota_report_to_json(results: list[tuple[str, QuotaResult]]) -> str:
    data = [
        {
            "pipeline": p,
            "kept": len(r.kept),
            "dropped": r.total_dropped,
            "dropped_rules": [a.rule_name for a in r.dropped],
        }
        for p, r in results
    ]
    return json.dumps(data, indent=2)
