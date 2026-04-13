"""Export pipeline metrics and reports to various output formats (CSV, JSON Lines)."""

from __future__ import annotations

import csv
import io
import json
from typing import Iterable

from pipewatch.history import MetricSnapshot


_CSV_FIELDS = [
    "pipeline",
    "timestamp",
    "total_rows",
    "passed_rows",
    "failed_rows",
    "success_rate",
    "throughput_per_second",
    "is_healthy",
]


def snapshots_to_csv(snapshots: Iterable[MetricSnapshot]) -> str:
    """Serialise an iterable of MetricSnapshot objects to a CSV string."""
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=_CSV_FIELDS, extrasaction="ignore")
    writer.writeheader()
    for snap in snapshots:
        writer.writerow(
            {
                "pipeline": snap.pipeline,
                "timestamp": snap.timestamp.isoformat(),
                "total_rows": snap.total_rows,
                "passed_rows": snap.passed_rows,
                "failed_rows": snap.failed_rows,
                "success_rate": round(snap.success_rate, 6),
                "throughput_per_second": round(snap.throughput_per_second, 6),
                "is_healthy": snap.is_healthy,
            }
        )
    return buf.getvalue()


def snapshots_to_jsonl(snapshots: Iterable[MetricSnapshot]) -> str:
    """Serialise an iterable of MetricSnapshot objects to a JSON Lines string."""
    lines: list[str] = []
    for snap in snapshots:
        record = {
            "pipeline": snap.pipeline,
            "timestamp": snap.timestamp.isoformat(),
            "total_rows": snap.total_rows,
            "passed_rows": snap.passed_rows,
            "failed_rows": snap.failed_rows,
            "success_rate": snap.success_rate,
            "throughput_per_second": snap.throughput_per_second,
            "is_healthy": snap.is_healthy,
        }
        lines.append(json.dumps(record))
    return "\n".join(lines) + ("\n" if lines else "")


def export_history(history, fmt: str = "csv") -> str:
    """Export all snapshots from a PipelineHistory in the requested format.

    Args:
        history: A PipelineHistory instance.
        fmt: Either ``"csv"`` or ``"jsonl"``.

    Returns:
        Serialised string in the chosen format.

    Raises:
        ValueError: If *fmt* is not recognised.
    """
    snaps = history.last_n(len(history._snapshots))  # type: ignore[attr-defined]
    if fmt == "csv":
        return snapshots_to_csv(snaps)
    if fmt == "jsonl":
        return snapshots_to_jsonl(snaps)
    raise ValueError(f"Unsupported export format: {fmt!r}. Choose 'csv' or 'jsonl'.")
