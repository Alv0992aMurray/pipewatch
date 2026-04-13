"""Formatting helpers for SLA evaluation results."""
from __future__ import annotations

import json
from typing import List

from pipewatch.sla import SLAResult

_PASS = "\u2705"
_FAIL = "\u274c"


def format_sla_result(result: SLAResult) -> str:
    """Return a human-readable single-pipeline SLA summary."""
    icon = _PASS if result.passed else _FAIL
    lines = [f"{icon} SLA [{result.pipeline}]: {'PASSED' if result.passed else 'FAILED'}"]
    for v in result.violations:
        lines.append(f"   - {v.message}")
    return "\n".join(lines)


def format_sla_report(results: List[SLAResult]) -> str:
    """Return a formatted report for multiple pipeline SLA results."""
    if not results:
        return "No SLA results to report."
    sections = ["=== SLA Report ==="]
    passed = sum(1 for r in results if r.passed)
    sections.append(f"Pipelines checked: {len(results)}  |  Passed: {passed}  |  Failed: {len(results) - passed}")
    sections.append("")
    for result in results:
        sections.append(format_sla_result(result))
    return "\n".join(sections)


def sla_report_to_json(results: List[SLAResult]) -> str:
    """Serialise SLA results to a JSON string."""
    return json.dumps([r.to_dict() for r in results], indent=2)
