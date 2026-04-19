"""Formatting helpers for ChurnResult."""
from __future__ import annotations
import json
from typing import List
from pipewatch.churn import ChurnResult


def _flag(result: ChurnResult) -> str:
    return "⚡" if result.is_churning else "✔"


def format_churn_result(result: ChurnResult) -> str:
    flag = _flag(result)
    lines = [
        f"{flag} [{result.pipeline}] churn_rate={result.churn_rate:.2%} "
        f"transitions={result.transitions}/{result.window_size - 1}",
    ]
    if result.note:
        lines.append(f"   ↳ {result.note}")
    return "\n".join(lines)


def format_churn_report(results: List[ChurnResult]) -> str:
    if not results:
        return "No churn data available."
    return "\n".join(format_churn_result(r) for r in results)


def churn_report_to_json(results: List[ChurnResult]) -> str:
    return json.dumps([r.to_dict() for r in results], indent=2)
