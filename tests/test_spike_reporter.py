"""Tests for pipewatch.spike_reporter."""
import json
from pipewatch.spike import SpikeResult
from pipewatch.spike_reporter import (
    format_spike_result,
    format_spike_report,
    spike_report_to_json,
)


def _result(is_spike: bool = False) -> SpikeResult:
    return SpikeResult(
        pipeline="orders",
        metric="error_rate",
        current_value=0.5 if is_spike else 0.05,
        baseline_mean=0.05,
        ratio=10.0 if is_spike else 1.0,
        is_spike=is_spike,
        note="ratio 10.00 exceeds threshold 2.0" if is_spike else "",
    )


def test_format_result_ok_contains_checkmark():
    out = format_spike_result(_result(is_spike=False))
    assert "✅" in out


def test_format_result_spike_contains_flag():
    out = format_spike_result(_result(is_spike=True))
    assert "🔺" in out


def test_format_result_contains_pipeline():
    out = format_spike_result(_result())
    assert "orders" in out


def test_format_result_contains_ratio():
    out = format_spike_result(_result(is_spike=True))
    assert "10.00" in out


def test_format_result_shows_note_when_present():
    out = format_spike_result(_result(is_spike=True))
    assert "exceeds threshold" in out


def test_format_result_omits_note_when_absent():
    out = format_spike_result(_result(is_spike=False))
    assert "note" not in out


def test_format_report_empty_shows_message():
    out = format_spike_report([])
    assert "No spike data" in out


def test_format_report_with_none_values():
    out = format_spike_report([None, None])
    assert "No spike data" in out


def test_format_report_multiple_results():
    results = [_result(False), _result(True)]
    out = format_spike_report(results)
    assert "✅" in out
    assert "🔺" in out


def test_json_output_is_valid():
    results = [_result(True)]
    raw = spike_report_to_json(results)
    parsed = json.loads(raw)
    assert len(parsed) == 1
    assert parsed[0]["is_spike"] is True
