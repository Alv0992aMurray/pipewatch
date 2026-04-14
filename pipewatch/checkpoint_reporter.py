"""Formatting helpers for checkpoint data."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Optional

from pipewatch.checkpoint import CheckpointEntry, CheckpointStore


def _age_label(seconds: Optional[float]) -> str:
    if seconds is None:
        return "never run"
    if seconds < 60:
        return f"{int(seconds)}s ago"
    if seconds < 3600:
        return f"{int(seconds // 60)}m ago"
    return f"{int(seconds // 3600)}h ago"


def format_entry(entry: CheckpointEntry, now: Optional[datetime] = None) -> str:
    ref = now or datetime.now(timezone.utc)
    age = (ref - entry.last_run).total_seconds()
    status_marker = "✓" if entry.last_status == "ok" else "✗"
    return (
        f"  {status_marker} {entry.pipeline:<30} "
        f"last run: {_age_label(age)}  "
        f"runs: {entry.run_count}  "
        f"status: {entry.last_status}"
    )


def format_checkpoint_report(store: CheckpointStore, now: Optional[datetime] = None) -> str:
    entries = store.all_entries()
    if not entries:
        return "Checkpoints: no pipelines recorded."
    lines = ["Checkpoint Report", "=" * 60]
    for entry in sorted(entries, key=lambda e: e.pipeline):
        lines.append(format_entry(entry, now=now))
    return "\n".join(lines)


def checkpoint_report_to_json(store: CheckpointStore) -> str:
    return json.dumps(
        {"checkpoints": [e.to_dict() for e in store.all_entries()]},
        indent=2,
    )
