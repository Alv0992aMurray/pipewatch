"""Formatting utilities for heartbeat results."""
from __future__ import annotations

import json
from typing import List

from pipewatch.heartbeat import HeartbeatResult


def _age_str(seconds: float) -> str:
    if seconds < 60:
        return f"{int(seconds)}s"
    if seconds < 3600:
        return f"{int(seconds // 60)}m {int(seconds % 60)}s"
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    return f"{hours}h {minutes}m"


def format_heartbeat_result(result: HeartbeatResult) -> str:
    status = "MISSED" if result.missed else "OK"
    icon = "✗" if result.missed else "✓"
    if result.last_seen is None:
        age = "never seen"
    else:
        assert result.seconds_since_last is not None
        age = f"last seen {_age_str(result.seconds_since_last)} ago"
    interval = _age_str(result.expected_interval_seconds)
    return f"[{icon}] {result.pipeline} — {status} ({age}, expected every {interval})"


def format_heartbeat_report(results: List[HeartbeatResult]) -> str:
    if not results:
        return "Heartbeat: no pipelines configured."
    lines = ["Heartbeat Report", "=" * 40]
    missed = sum(1 for r in results if r.missed)
    for r in results:
        lines.append(format_heartbeat_result(r))
    lines.append("")
    lines.append(f"Total: {len(results)} pipeline(s), {missed} missed.")
    return "\n".join(lines)


def heartbeat_report_to_json(results: List[HeartbeatResult]) -> str:
    return json.dumps([r.to_dict() for r in results], indent=2)
