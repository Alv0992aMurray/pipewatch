"""Formatting helpers for DriftResult."""
from __future__ import annotations

import json
from typing import List, Optional

from pipewatch.drift import DriftResult


def _pct(value: Optional[float]) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:+.2f}%"


def _fmt(value: Optional[float]) -> str:
    if value is None:
        return "n/a"
    return f"{value:.4f}"


def format_drift_result(result: DriftResult) -> str:
    status = "DRIFT DETECTED" if result.drifted else "stable"
    lines = [
        f"Pipeline : {result.pipeline}",
        f"Metric   : {result.metric}",
        f"Status   : {status}",
        f"Reference mean : {_fmt(result.reference_mean)}",
        f"Recent mean    : {_fmt(result.recent_mean)}",
        f"Delta          : {_fmt(result.delta)}",
        f"Relative change: {_pct(result.relative_change)}",
        f"Threshold      : {_pct(result.threshold)}",
    ]
    return "\n".join(lines)


def format_drift_report(results: List[DriftResult]) -> str:
    if not results:
        return "No drift results available."
    sections = [format_drift_result(r) for r in results]
    return "\n\n".join(sections)


def drift_report_to_json(results: List[DriftResult]) -> str:
    return json.dumps([r.to_dict() for r in results], indent=2)
