"""Formatting helpers for SampleWindow results."""
from __future__ import annotations

import json
from typing import List

from pipewatch.sampling import SampleWindow


def format_sample_window(window: SampleWindow) -> str:
    """Return a human-readable summary of a sample window."""
    d = window.to_dict()
    pipeline = d["pipeline"]
    count = d["sample_count"]
    win = d["window_seconds"]

    if window.is_empty():
        return f"[{pipeline}] No samples in {win}s window."

    sr = d["average_success_rate"]
    tp = d["average_throughput"]

    sr_str = f"{sr:.1%}" if sr is not None else "n/a"
    tp_str = f"{tp:.1f} rows/s" if tp is not None else "n/a"

    return (
        f"[{pipeline}] {count} sample(s) over {win}s window | "
        f"avg success rate: {sr_str} | avg throughput: {tp_str}"
    )


def format_sampling_report(windows: List[SampleWindow]) -> str:
    """Return a multi-line report for a list of sample windows."""
    if not windows:
        return "No sample windows to report."
    lines = ["=== Sampling Report ==="]
    for w in windows:
        lines.append(format_sample_window(w))
    return "\n".join(lines)


def sampling_report_to_json(windows: List[SampleWindow]) -> str:
    """Serialise all sample windows to a JSON string."""
    return json.dumps([w.to_dict() for w in windows], indent=2)
