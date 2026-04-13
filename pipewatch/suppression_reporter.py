"""Formatting helpers for suppression results."""

from __future__ import annotations

import json
from typing import List

from pipewatch.suppression import SuppressionResult, SuppressionRule


def format_suppression_result(result: SuppressionResult) -> str:
    lines: List[str] = []
    if result.suppressed:
        lines.append(f"  Suppressed alerts ({len(result.suppressed)}):")
        for a in result.suppressed:
            lines.append(f"    - [{a.severity.value.upper()}] {a.pipeline} / {a.rule_name}: {a.message}")
    if result.kept:
        lines.append(f"  Active alerts ({len(result.kept)}):")
        for a in result.kept:
            lines.append(f"    - [{a.severity.value.upper()}] {a.pipeline} / {a.rule_name}: {a.message}")
    if not result.suppressed and not result.kept:
        lines.append("  No alerts.")
    return "\n".join(lines)


def format_suppression_rules(rules: List[SuppressionRule]) -> str:
    if not rules:
        return "  No active suppression rules."
    lines = ["  Active suppression rules:"]
    for r in rules:
        status = "active" if r.is_active() else "expired"
        lines.append(f"    - {r.pipeline}/{r.rule_name} until {r.until:.0f} ({status}): {r.reason}")
    return "\n".join(lines)


def suppression_report_to_json(result: SuppressionResult) -> str:
    return json.dumps(
        {
            "kept": [
                {"pipeline": a.pipeline, "rule_name": a.rule_name, "message": a.message, "severity": a.severity.value}
                for a in result.kept
            ],
            "suppressed": [
                {"pipeline": a.pipeline, "rule_name": a.rule_name, "message": a.message, "severity": a.severity.value}
                for a in result.suppressed
            ],
        },
        indent=2,
    )
