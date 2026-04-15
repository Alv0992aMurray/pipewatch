"""Formatting helpers for fingerprinted alert reports."""

from __future__ import annotations

import json

from pipewatch.fingerprint import FingerprintedAlert, group_by_fingerprint
from pipewatch.alerts import Alert


def format_fingerprinted_alert(fa: FingerprintedAlert) -> str:
    """Return a single-line summary for a fingerprinted alert."""
    fp = fa.fingerprint
    return (
        f"[{fp.severity.upper()}] {fp.pipeline} / {fp.rule_name} "
        f"(#{fp.digest}) — {fa.alert.message}"
    )


def format_fingerprint_report(alerts: list[Alert]) -> str:
    """Return a human-readable report showing alerts grouped by fingerprint."""
    if not alerts:
        return "No alerts to fingerprint."

    groups = group_by_fingerprint(alerts)
    lines: list[str] = [f"Fingerprint Report ({len(groups)} unique alert type(s)):", ""]

    for fp_str, group in groups.items():
        count = len(group)
        sample = group[0]
        severity = sample.severity.value if hasattr(sample.severity, "value") else str(sample.severity)
        lines.append(f"  [{severity.upper()}] {fp_str}")
        lines.append(f"    Rule    : {sample.rule_name}")
        lines.append(f"    Pipeline: {sample.pipeline}")
        lines.append(f"    Count   : {count}")
        lines.append(f"    Message : {sample.message}")
        lines.append("")

    return "\n".join(lines).rstrip()


def fingerprint_report_to_json(alerts: list[Alert]) -> str:
    """Return a JSON string of grouped fingerprinted alerts."""
    groups = group_by_fingerprint(alerts)
    payload = []
    for fp_str, group in groups.items():
        sample = group[0]
        severity = sample.severity.value if hasattr(sample.severity, "value") else str(sample.severity)
        payload.append({
            "fingerprint": fp_str,
            "pipeline": sample.pipeline,
            "rule_name": sample.rule_name,
            "severity": severity,
            "count": len(group),
            "sample_message": sample.message,
        })
    return json.dumps(payload, indent=2)
