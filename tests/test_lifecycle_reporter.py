"""Tests for pipewatch.lifecycle_reporter."""
import json
from datetime import datetime, timezone
from pipewatch.lifecycle import LifecycleState, LifecycleResult
from pipewatch.lifecycle_reporter import (
    format_lifecycle_state,
    format_lifecycle_report,
    lifecycle_report_to_json,
)


def _ts() -> datetime:
    return datetime(2024, 6, 1, 9, 0, 0, tzinfo=timezone.utc)


def test_format_state_healthy_contains_checkmark():
    s = LifecycleState("pipe1", "healthy", _ts())
    out = format_lifecycle_state(s)
    assert "✅" in out
    assert "pipe1" in out
    assert "HEALTHY" in out


def test_format_state_degraded_contains_red_circle():
    s = LifecycleState("pipe2", "degraded", _ts())
    out = format_lifecycle_state(s)
    assert "🔴" in out


def test_format_state_shows_previous_when_present():
    s = LifecycleState("pipe1", "recovering", _ts(), previous="degraded")
    out = format_lifecycle_state(s)
    assert "was: degraded" in out


def test_format_state_omits_previous_when_absent():
    s = LifecycleState("pipe1", "healthy", _ts())
    out = format_lifecycle_state(s)
    assert "was:" not in out


def test_format_report_empty_shows_message():
    result = LifecycleResult()
    out = format_lifecycle_report(result)
    assert "No lifecycle" in out


def test_format_report_lists_all_pipelines():
    result = LifecycleResult(states=[
        LifecycleState("a", "healthy", _ts()),
        LifecycleState("b", "degraded", _ts()),
    ])
    out = format_lifecycle_report(result)
    assert "a" in out
    assert "b" in out


def test_lifecycle_report_to_json_is_valid():
    result = LifecycleResult(states=[
        LifecycleState("x", "unknown", _ts()),
    ])
    raw = lifecycle_report_to_json(result)
    data = json.loads(raw)
    assert "states" in data
    assert data["states"][0]["pipeline"] == "x"
