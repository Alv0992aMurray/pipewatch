"""Reporting utilities for watermark results."""
from __future__ import annotations

import json
from typing import List

from pipewatch.watermark import WatermarkEntry, WatermarkResult


def format_watermark_entry(entry: WatermarkEntry) -> str:
    ts = entry.recorded_at.strftime("%Y-%m-%d %H:%M:%S")
    return (
        f"  [{entry.pipeline}] {entry.metric}: "
        f"peak={entry.peak_value:.4f} "
        f"(recorded {ts}, over {entry.snapshot_count} snapshots)"
    )


def format_watermark_report(result: WatermarkResult) -> str:
    lines: List[str] = ["=== High-Water Marks ==="]
    if not result.entries:
        lines.append("  No watermarks recorded.")
        return "\n".join(lines)

    by_pipeline: dict = {}
    for entry in result.entries:
        by_pipeline.setdefault(entry.pipeline, []).append(entry)

    for pipeline, entries in sorted(by_pipeline.items()):
        lines.append(f"Pipeline: {pipeline}")
        for entry in entries:
            lines.append(format_watermark_entry(entry))

    return "\n".join(lines)


def watermark_report_to_json(result: WatermarkResult) -> str:
    return json.dumps(result.to_dict(), indent=2)
