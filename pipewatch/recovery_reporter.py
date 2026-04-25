"""Formatting helpers for RecoveryResult."""
from __future__ import annotations

import json
from typing import List

from pipewatch.recovery import RecoveryResult


def _flag(recovered: bool) -> str:
    return "\u2705" if recovered else "\u23f3"


def format_recovery_result(result: RecoveryResult) -> str:
    icon = _flag(result.recovered)
    status = "RECOVERED" if result.recovered else "NOT RECOVERED"
    lines = [
        f"{icon} [{result.pipeline}] {status}",
        f"   prior failures : {result.previous_failures}",
        f"   note           : {result.note}",
    ]
    if result.recovery_snapshot_index is not None:
        lines.append(f"   snapshot index : {result.recovery_snapshot_index}")
    return "\n".join(lines)


def format_recovery_report(results: List[RecoveryResult]) -> str:
    if not results:
        return "recovery report: no pipelines analysed"
    lines = ["=== Recovery Report ==="]
    for r in results:
        lines.append(format_recovery_result(r))
    recovered_count = sum(1 for r in results if r.recovered)
    lines.append(f"--- {recovered_count}/{len(results)} pipeline(s) recovered ---")
    return "\n".join(lines)


def recovery_report_to_json(results: List[RecoveryResult]) -> str:
    return json.dumps([r.to_dict() for r in results], indent=2)
