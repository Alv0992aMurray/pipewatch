"""Formatting helpers for segmentation results."""
from __future__ import annotations
import json
from pipewatch.segmentation import SegmentationResult


def _fmt_pct(value: float) -> str:
    return f"{value * 100:.1f}%"


def format_segment(segment) -> str:
    health = _fmt_pct(segment.avg_success_rate)
    ratio = f"{segment.healthy_count}/{segment.count}"
    return f"  [{segment.name}]  pipelines: {segment.count}  healthy: {ratio}  avg success: {health}"


def format_segmentation_report(result: SegmentationResult) -> str:
    if not result.segments:
        return "No segments defined."
    lines = ["=== Pipeline Segmentation ==="]
    for seg in result.segments:
        lines.append(format_segment(seg))
    if result.unmatched:
        names = ", ".join(m.pipeline_name for m in result.unmatched)
        lines.append(f"  [unmatched]  pipelines: {len(result.unmatched)}  ({names})")
    return "\n".join(lines)


def segmentation_report_to_json(result: SegmentationResult) -> str:
    data = {
        "segments": [s.to_dict() for s in result.segments],
        "unmatched_count": len(result.unmatched),
        "total_metrics": result.total_metrics,
    }
    return json.dumps(data, indent=2)
