"""Formatting helpers for JitterResult."""
from __future__ import annotations

import json
from typing import List, Optional

from pipewatch.jitter import JitterResult


def _flag(irregular: bool) -> str:
    return "⚠️  IRREGULAR" if irregular else "✅ REGULAR"


def format_jitter_result(result: JitterResult) -> str:
    lines = [
        f"Pipeline : {result.pipeline}",
        f"Status   : {_flag(result.is_irregular)}",
        f"Samples  : {result.sample_count}",
        f"Mean Δt  : {result.mean_interval_seconds:.1f}s",
        f"Std Dev  : {result.stddev_seconds:.1f}s",
        f"Ratio    : {result.jitter_ratio:.4f}",
    ]
    if result.note:
        lines.append(f"Note     : {result.note}")
    return "\n".join(lines)


def format_jitter_report(results: List[Optional[JitterResult]]) -> str:
    valid = [r for r in results if r is not None]
    if not valid:
        return "No jitter data available."
    sections = [format_jitter_result(r) for r in valid]
    return "\n\n".join(sections)


def jitter_report_to_json(results: List[Optional[JitterResult]]) -> str:
    valid = [r for r in results if r is not None]
    return json.dumps([r.to_dict() for r in valid], indent=2)
