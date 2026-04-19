"""Reporting helpers for outlier detection results."""
from __future__ import annotations
import json
from typing import Optional
from pipewatch.outlier import OutlierResult


def _flag(result: OutlierResult) -> str:
    if not result.is_outlier:
        return "\u2705"
    return "\u26a0\ufe0f" if result.direction == "low" else "\U0001f4c8"


def format_outlier_result(result: OutlierResult) -> str:
    icon = _flag(result)
    status = f"OUTLIER ({result.direction})" if result.is_outlier else "OK"
    lines = [
        f"{icon} [{result.pipeline}] {result.metric}: {result.value:.4f} — {status}",
        f"   mean={result.mean:.4f}  fences=[{result.lower_fence:.4f}, {result.upper_fence:.4f}]",
    ]
    return "\n".join(lines)


def format_outlier_report(results: list[Optional[OutlierResult]]) -> str:
    valid = [r for r in results if r is not None]
    if not valid:
        return "No outlier data available."
    return "\n".join(format_outlier_result(r) for r in valid)


def outlier_report_to_json(results: list[Optional[OutlierResult]]) -> str:
    valid = [r for r in results if r is not None]
    return json.dumps([r.to_dict() for r in valid], indent=2)
