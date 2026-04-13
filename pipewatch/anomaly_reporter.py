"""Formatting helpers for anomaly detection results."""

from __future__ import annotations

import json
from typing import List

from pipewatch.anomaly import AnomalyResult


def _flag(result: AnomalyResult) -> str:
    return "[ANOMALY]" if result.is_anomaly else "[OK]    "


def format_anomaly_result(result: AnomalyResult) -> str:
    """Return a single-line human-readable summary for one anomaly result."""
    return (
        f"{_flag(result)} {result.pipeline} / {result.metric}: "
        f"{result.message}"
    )


def format_anomaly_report(results: List[AnomalyResult]) -> str:
    """Return a multi-line report for a list of anomaly results."""
    if not results:
        return "No anomaly results to display."

    lines = ["=== Anomaly Detection Report ==="]
    for r in results:
        lines.append(format_anomaly_result(r))

    anomalies = [r for r in results if r.is_anomaly]
    lines.append("")
    lines.append(
        f"Summary: {len(anomalies)} anomal{'y' if len(anomalies) == 1 else 'ies'} "
        f"detected out of {len(results)} check(s)."
    )
    return "\n".join(lines)


def anomaly_report_to_json(results: List[AnomalyResult]) -> str:
    """Serialise anomaly results to a JSON string."""
    return json.dumps([r.to_dict() for r in results], indent=2)
