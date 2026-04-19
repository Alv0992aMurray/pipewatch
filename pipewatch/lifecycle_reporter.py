"""Formatting helpers for lifecycle state results."""
from __future__ import annotations
import json
from pipewatch.lifecycle import LifecycleResult, LifecycleState

_ICONS = {
    "healthy": "✅",
    "degraded": "🔴",
    "recovering": "🟡",
    "unknown": "❓",
}


def _icon(state: str) -> str:
    return _ICONS.get(state, "❓")


def format_lifecycle_state(state: LifecycleState) -> str:
    icon = _icon(state.state)
    line = f"{icon} {state.pipeline}: {state.state.upper()} (since {state.since.strftime('%Y-%m-%d %H:%M:%S')} UTC)"
    if state.previous:
        line += f" [was: {state.previous}]"
    return line


def format_lifecycle_report(result: LifecycleResult) -> str:
    if not result.states:
        return "No lifecycle states recorded."
    lines = ["── Pipeline Lifecycle ──"]
    for state in result.states:
        lines.append(format_lifecycle_state(state))
    return "\n".join(lines)


def lifecycle_report_to_json(result: LifecycleResult) -> str:
    return json.dumps(result.to_dict(), indent=2)
