"""Format baseline comparison results for CLI output and JSON export."""

from __future__ import annotations

import json
from typing import List

from pipewatch.baseline import BaselineDelta


def _sign(value: float) -> str:
    return "+" if value >= 0 else ""


def format_delta(delta: BaselineDelta) -> str:
    """Return a human-readable single-line summary of a BaselineDelta."""
    status = "[REGRESSED]" if delta.regressed else "[OK]"
    sr_str = f"{_sign(delta.success_rate_delta)}{delta.success_rate_delta:.2%}"
    tp_str = f"{_sign(delta.throughput_delta)}{delta.throughput_delta:.2f} rows/s"
    return (
        f"{status} {delta.pipeline_id} | "
        f"success_rate: {sr_str} | "
        f"throughput: {tp_str}"
    )


def format_baseline_report(deltas: List[BaselineDelta]) -> str:
    """Return a multi-line CLI report for a list of BaselineDeltas."""
    if not deltas:
        return "No baseline comparisons available."

    lines = ["=== Baseline Comparison ==="]
    for delta in deltas:
        lines.append(format_delta(delta))

    regressions = [d for d in deltas if d.regressed]
    lines.append("")
    if regressions:
        names = ", ".join(d.pipeline_id for d in regressions)
        lines.append(f"WARNING: {len(regressions)} pipeline(s) regressed: {names}")
    else:
        lines.append("All pipelines within baseline tolerance.")
    return "\n".join(lines)


def baseline_report_to_json(deltas: List[BaselineDelta]) -> str:
    """Serialise a list of BaselineDeltas to a JSON string."""
    payload = [
        {
            "pipeline_id": d.pipeline_id,
            "success_rate_delta": d.success_rate_delta,
            "throughput_delta": d.throughput_delta,
            "regressed": d.regressed,
        }
        for d in deltas
    ]
    return json.dumps({"baseline_comparison": payload}, indent=2)
