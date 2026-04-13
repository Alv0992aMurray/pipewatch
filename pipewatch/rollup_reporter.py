"""Rollup reporter: format RollupStats for CLI output and JSON export."""
from __future__ import annotations

import json
from typing import Optional

from pipewatch.rollup import RollupStats


def _fmt_pct(value: Optional[float]) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.1f}%"


def _fmt_float(value: Optional[float], unit: str = "") -> str:
    if value is None:
        return "n/a"
    return f"{value:.2f}{unit}"


def format_rollup(stats: RollupStats) -> str:
    """Return a human-readable multi-line summary of rollup statistics."""
    lines = [
        "=== Pipeline Rollup Summary ===",
        f"  Pipelines   : {stats.pipeline_count}",
        f"  Healthy     : {stats.healthy_count}  |  Unhealthy: {stats.unhealthy_count}",
        f"  Rows Processed : {stats.total_rows_processed:,}",
        f"  Total Errors   : {stats.total_errors:,}",
        "  Success Rate:",
        f"    avg={_fmt_pct(stats.avg_success_rate)}",
        f"    min={_fmt_pct(stats.min_success_rate)}",
        f"    max={_fmt_pct(stats.max_success_rate)}",
        f"  Avg Throughput : {_fmt_float(stats.avg_throughput, ' rows/s')}",
    ]
    return "\n".join(lines)


def rollup_to_json(stats: RollupStats) -> str:
    """Serialise RollupStats to a JSON string."""
    return json.dumps(stats.to_dict(), indent=2)
