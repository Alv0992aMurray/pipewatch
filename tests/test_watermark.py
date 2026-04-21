"""Tests for pipewatch.watermark and pipewatch.watermark_reporter."""
from datetime import datetime, timezone

import pytest

from pipewatch.history import MetricSnapshot
from pipewatch.watermark import WatermarkEntry, WatermarkResult, compute_watermarks
from pipewatch.watermark_reporter import (
    format_watermark_entry,
    format_watermark_report,
    watermark_report_to_json,
)


def _ts(offset: int = 0) -> datetime:
    return datetime(2024, 1, 1, 12, offset, 0, tzinfo=timezone.utc)


def _snap(pipeline: str, success_rate: float, throughput: float, offset: int = 0) -> MetricSnapshot:
    return MetricSnapshot(
        pipeline=pipeline,
        timestamp=_ts(offset),
        success_rate=success_rate,
        throughput=throughput,
        error_rate=1.0 - success_rate,
        is_healthy=success_rate >= 0.9,
    )


def test_empty_snapshots_returns_empty_result():
    result = compute_watermarks([])
    assert result.entries == []


def test_single_snapshot_records_peak():
    snap = _snap("pipe_a", 0.95, 100.0)
    result = compute_watermarks([snap])
    entry = result.get("pipe_a", "success_rate")
    assert entry is not None
    assert entry.peak_value == pytest.approx(0.95)


def test_highest_value_is_retained():
    snaps = [
        _snap("pipe_a", 0.80, 50.0, offset=0),
        _snap("pipe_a", 0.99, 80.0, offset=1),
        _snap("pipe_a", 0.70, 120.0, offset=2),
    ]
    result = compute_watermarks(snaps)
    entry = result.get("pipe_a", "success_rate")
    assert entry.peak_value == pytest.approx(0.99)


def test_throughput_watermark_tracked_separately():
    snaps = [
        _snap("pipe_a", 0.90, 200.0, offset=0),
        _snap("pipe_a", 0.95, 50.0, offset=1),
    ]
    result = compute_watermarks(snaps)
    tp_entry = result.get("pipe_a", "throughput")
    assert tp_entry.peak_value == pytest.approx(200.0)


def test_multiple_pipelines_tracked_independently():
    snaps = [
        _snap("pipe_a", 0.99, 100.0),
        _snap("pipe_b", 0.50, 200.0),
    ]
    result = compute_watermarks(snaps)
    assert result.get("pipe_a", "success_rate").peak_value == pytest.approx(0.99)
    assert result.get("pipe_b", "success_rate").peak_value == pytest.approx(0.50)


def test_to_dict_contains_expected_keys():
    entry = WatermarkEntry(pipeline="p", metric="success_rate", peak_value=0.95, recorded_at=_ts())
    d = entry.to_dict()
    assert "pipeline" in d
    assert "metric" in d
    assert "peak_value" in d
    assert "recorded_at" in d


def test_format_watermark_report_empty():
    result = WatermarkResult()
    report = format_watermark_report(result)
    assert "No watermarks" in report


def test_format_watermark_report_contains_pipeline():
    snap = _snap("my_pipeline", 0.88, 75.0)
    result = compute_watermarks([snap])
    report = format_watermark_report(result)
    assert "my_pipeline" in report


def test_watermark_report_to_json_is_valid_json():
    snap = _snap("pipe_a", 0.91, 60.0)
    result = compute_watermarks([snap])
    import json
    data = json.loads(watermark_report_to_json(result))
    assert "watermarks" in data
