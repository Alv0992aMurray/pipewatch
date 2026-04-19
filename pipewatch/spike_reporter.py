"""Formatting helpers for SpikeResult."""
from __future__ import annotations
import json
from typing import List, Optional
from pipewatch.spike import SpikeResult


def _flag(result: SpikeResult) -> str:
    return "🔺 SPIKE" if result.is_spike else "✅ OK"


def format_spike_result(result: SpikeResult) -> str:
    lines = [
        f"[{_flag(result)}] {result.pipeline} — {result.metric}",
        f"  current : {result.current_value:.4f}",
        f"  baseline: {result.baseline_mean:.4f}",
        f"  ratio   : {result.ratio:.2f}x",
    ]
    if result.note:
        lines.append(f"  note    : {result.note}")
    return "\n".join(lines)


def format_spike_report(results: List[Optional[SpikeResult]]) -> str:
    valid = [r for r in results if r is not None]
    if not valid:
        return "No spike data available."
    return "\n\n".join(format_spike_result(r) for r in valid)


def spike_report_to_json(results: List[Optional[SpikeResult]]) -> str:
    valid = [r for r in results if r is not None]
    return json.dumps([r.to_dict() for r in valid], indent=2)
