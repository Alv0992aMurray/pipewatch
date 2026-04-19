"""Tests for pipewatch.latency_reporter."""
import json

from pipewatch.latency import LatencyResult
from pipewatch.latency_reporter import (
    format_latency_result,
    format_latency_report,
    latency_report_to_json,
)


def _result(is_high: bool = False, avg: float = 60.0) -> LatencyResult:
    return LatencyResult(
        pipeline="etl_pipe",
        sample_count=5,
        min_seconds=30.0,
        max_seconds=120.0,
        avg_seconds=avg,
        is_high=is_high,
        threshold_seconds=300.0,
    )


def test_format_result_ok_contains_checkmark():
    out = format_latency_result(_result(is_high=False))
    assert "✓ OK" in out


def test_format_result_high_contains_warning():
    out = format_latency_result(_result(is_high=True))
    assert "⚠ HIGH" in out


def test_format_result_contains_pipeline_name():
    out = format_latency_result(_result())
    assert "etl_pipe" in out


def test_format_result_contains_avg():
    out = format_latency_result(_result(avg=60.0))
    assert "60.0s" in out


def test_format_report_empty_returns_message():
    out = format_latency_report([])
    assert "No latency" in out


def test_format_report_multiple_results():
    results = [_result(is_high=False), _result(is_high=True)]
    out = format_latency_report(results)
    assert "✓ OK" in out
    assert "⚠ HIGH" in out


def test_json_output_is_valid():
    results = [_result()]
    raw = latency_report_to_json(results)
    parsed = json.loads(raw)
    assert isinstance(parsed, list)
    assert parsed[0]["pipeline"] == "etl_pipe"
