"""Tests for pipewatch.pattern_reporter."""
from __future__ import annotations

import json
import pytest

from pipewatch.pattern import PatternResult
from pipewatch.pattern_reporter import (
    format_pattern_result,
    format_pattern_report,
    pattern_report_to_json,
)


def _result(label: str, note: str | None = None) -> PatternResult:
    return PatternResult(
        pipeline="orders",
        metric="success_rate",
        total_snapshots=10,
        failure_runs=3,
        consecutive_failures=2,
        alternating=False,
        pattern_label=label,
        note=note,
    )


def test_format_result_contains_pipeline():
    out = format_pattern_result(_result("degraded"))
    assert "orders" in out


def test_format_result_contains_label():
    out = format_pattern_result(_result("sustained_failure"))
    assert "sustained_failure" in out


def test_format_result_shows_note_when_present():
    out = format_pattern_result(_result("degraded", note="3/10 runs failed"))
    assert "3/10 runs failed" in out


def test_format_result_omits_note_when_absent():
    out = format_pattern_result(_result("stable_healthy", note=None))
    assert "note" not in out


def test_format_result_stable_has_ok_icon():
    out = format_pattern_result(_result("stable_healthy"))
    assert "✅" in out


def test_format_result_sustained_has_alarm_icon():
    out = format_pattern_result(_result("sustained_failure"))
    assert "🚨" in out


def test_format_report_empty():
    out = format_pattern_report([])
    assert "No pattern data" in out


def test_format_report_lists_all_results():
    results = [_result("stable_healthy"), _result("flapping")]
    out = format_pattern_report(results)
    assert "2 pipeline" in out
    assert "stable_healthy" in out
    assert "flapping" in out


def test_json_output_is_valid():
    results = [_result("degraded", note="high failure rate")]
    raw = pattern_report_to_json(results)
    parsed = json.loads(raw)
    assert isinstance(parsed, list)
    assert parsed[0]["pipeline"] == "orders"
    assert parsed[0]["pattern_label"] == "degraded"


def test_json_empty_list():
    raw = pattern_report_to_json([])
    assert json.loads(raw) == []
