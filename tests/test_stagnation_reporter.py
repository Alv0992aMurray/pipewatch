"""Tests for pipewatch.stagnation_reporter."""

from __future__ import annotations

import json

from pipewatch.stagnation import StagnationResult
from pipewatch.stagnation_reporter import (
    format_stagnation_result,
    format_stagnation_report,
    stagnation_report_to_json,
)


def _ok() -> StagnationResult:
    return StagnationResult(
        pipeline="orders",
        is_stagnant=False,
        snapshot_count=8,
        unique_values=6,
        variance=0.0045,
        note="",
    )


def _stagnant() -> StagnationResult:
    return StagnationResult(
        pipeline="inventory",
        is_stagnant=True,
        snapshot_count=8,
        unique_values=1,
        variance=0.0,
        note="metric appears frozen",
    )


def test_format_result_ok_contains_checkmark():
    out = format_stagnation_result(_ok())
    assert "\u2705" in out


def test_format_result_stagnant_contains_warning():
    out = format_stagnation_result(_stagnant())
    assert "\u26a0" in out


def test_format_result_contains_pipeline_name():
    out = format_stagnation_result(_ok())
    assert "orders" in out


def test_format_result_shows_variance():
    out = format_stagnation_result(_ok())
    assert "0.004500" in out


def test_format_result_shows_note_when_stagnant():
    out = format_stagnation_result(_stagnant())
    assert "frozen" in out


def test_format_result_omits_note_when_empty():
    out = format_stagnation_result(_ok())
    assert "note" not in out


def test_format_report_empty_shows_message():
    out = format_stagnation_report([])
    assert "no results" in out


def test_format_report_multiple_results():
    out = format_stagnation_report([_ok(), _stagnant()])
    assert "orders" in out
    assert "inventory" in out


def test_json_output_is_valid():
    out = stagnation_report_to_json([_ok(), _stagnant()])
    data = json.loads(out)
    assert isinstance(data, list)
    assert len(data) == 2
    assert data[0]["pipeline"] == "orders"
    assert data[1]["is_stagnant"] is True
