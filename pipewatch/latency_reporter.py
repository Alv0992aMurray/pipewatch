"""Format latency results for CLI output."""
from __future__ import annotations

import json
from typing import List, Optional

from pipewatch.latency import LatencyResult


def _fmt(seconds: Optional[float]) -> str:
    if seconds is None:
        return "n/a"
    if seconds < 60:
        return f"{seconds:.1f}s"
    return f"{seconds / 60:.1f}m"


def format_latency_result(result: LatencyResult) -> str:
    flag = "⚠ HIGH" if result.is_high else "✓ OK"
    lines = [
        f"[{flag}] {result.pipeline}",
        f"  avg: {_fmt(result.avg_seconds)}  "
        f"min: {_fmt(result.min_seconds)}  "
        f"max: {_fmt(result.max_seconds)}",
        f"  threshold: {_fmt(result.threshold_seconds)}  samples: {result.sample_count}",
    ]
    return "\n".join(lines)


def format_latency_report(results: List[LatencyResult]) -> str:
    if not results:
        return "No latency data available."
    return "\n".join(format_latency_result(r) for r in results)


def latency_report_to_json(results: List[LatencyResult]) -> str:
    return json.dumps([r.to_dict() for r in results], indent=2)
