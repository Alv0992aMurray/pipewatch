"""Reporting helpers for stagnation detection results."""

from __future__ import annotations

import json
from typing import List

from pipewatch.stagnation import StagnationResult


def _flag(result: StagnationResult) -> str:
    return "\u26a0\ufe0f" if result.is_stagnant else "\u2705"


def format_stagnation_result(result: StagnationResult) -> str:
    icon = _flag(result)
    var_str = f"{result.variance:.6f}" if result.variance is not None else "n/a"
    lines = [
        f"{icon} [{result.pipeline}] stagnation check",
        f"   snapshots : {result.snapshot_count}",
        f"   unique values : {result.unique_values}",
        f"   variance  : {var_str}",
    ]
    if result.note:
        lines.append(f"   note      : {result.note}")
    return "\n".join(lines)


def format_stagnation_report(results: List[StagnationResult]) -> str:
    if not results:
        return "stagnation: no results"
    return "\n\n".join(format_stagnation_result(r) for r in results)


def stagnation_report_to_json(results: List[StagnationResult]) -> str:
    return json.dumps([r.to_dict() for r in results], indent=2)
