"""Reporter for CompactionResult."""
from __future__ import annotations

import json
from typing import List

from pipewatch.compaction import CompactionBucket, CompactionResult


def _fmt_pct(v: float) -> str:
    return f"{v * 100:.1f}%"


def _fmt_float(v: float) -> str:
    return f"{v:.2f}"


def format_bucket(bucket: CompactionBucket) -> str:
    flag = "⚠" if bucket.any_unhealthy else "✓"
    return (
        f"  [{flag}] {bucket.start.strftime('%H:%M')}–{bucket.end.strftime('%H:%M')} "
        f"| snaps={bucket.snapshot_count} "
        f"| success={_fmt_pct(bucket.avg_success_rate)} "
        f"| throughput={_fmt_float(bucket.avg_throughput)}"
    )


def format_compaction_report(results: List[CompactionResult]) -> str:
    if not results:
        return "No compaction results."
    lines = ["=== Compaction Report ==="]
    for r in results:
        lines.append(
            f"Pipeline: {r.pipeline} "
            f"(bucket={r.bucket_size_minutes}m, "
            f"buckets={r.total_buckets}, "
            f"retained={len(r.retained_snapshots)})"
        )
        if r.buckets:
            for b in r.buckets:
                lines.append(format_bucket(b))
        else:
            lines.append("  No compacted buckets.")
    return "\n".join(lines)


def compaction_report_to_json(results: List[CompactionResult]) -> str:
    return json.dumps([r.to_dict() for r in results], indent=2)
