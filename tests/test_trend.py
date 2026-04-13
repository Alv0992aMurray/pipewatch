"""Tests for pipewatch.trend module."""

from __future__ import annotations

import pytest

from pipewatch.history import MetricSnapshot, PipelineHistory
from pipewatch.trend import TrendReport, analyse_trend


def _make_snapshot(pipeline: str, success_rate: float) -> MetricSnapshot:
    return MetricSnapshot(
        pipeline=pipeline,
        timestamp="2024-01-01T00:00:00+00:00",
        success_rate=success_rate,
        throughput=100.0,
        error_count=0,
        is_healthy=success_rate >= 0.9,
    )


def _history_with_rates(rates: list[float], pipeline: str = "orders") -> PipelineHistory:
    h = PipelineHistory(pipeline=pipeline)
    for r in rates:
        h.add(_make_snapshot(pipeline, r))
    return h


def test_empty_history_returns_no_data():
    h = PipelineHistory(pipeline="orders")
    report = analyse_trend(h)
    assert report.sample_size == 0
    assert report.is_degrading is False
    assert report.is_recovering is False
    assert report.delta is None
    assert "no history" in report.summary_line()


def test_single_snapshot_no_trend():
    h = _history_with_rates([0.95])
    report = analyse_trend(h)
    assert report.sample_size == 1
    assert report.is_degrading is False
    assert report.is_recovering is False
    assert report.delta == 0.0


def test_stable_trend():
    h = _history_with_rates([0.95, 0.95, 0.95, 0.95, 0.95])
    report = analyse_trend(h)
    assert report.is_degrading is False
    assert report.is_recovering is False
    assert report.delta == pytest.approx(0.0, abs=0.001)


def test_degrading_trend():
    # prior avg ~0.95, latest drops to 0.80 => delta = -0.15
    h = _history_with_rates([0.95, 0.95, 0.95, 0.95, 0.80])
    report = analyse_trend(h)
    assert report.is_degrading is True
    assert report.is_recovering is False
    assert report.delta < -0.05


def test_recovering_trend():
    # prior avg ~0.70, latest jumps to 0.95 => delta = +0.25
    h = _history_with_rates([0.70, 0.70, 0.70, 0.70, 0.95])
    report = analyse_trend(h)
    assert report.is_recovering is True
    assert report.is_degrading is False
    assert report.delta > 0.05


def test_custom_window_uses_only_last_n():
    # Only last 3 entries should be considered
    h = _history_with_rates([0.50, 0.50, 0.95, 0.95, 0.40])
    report = analyse_trend(h, window=3)
    assert report.sample_size == 3
    # prior of window=[0.95, 0.95, 0.40] -> prior=[0.95,0.95], avg=0.95, latest=0.40
    assert report.is_degrading is True


def test_custom_degradation_threshold():
    # delta is -0.03, below default 0.05 but above 0.02
    h = _history_with_rates([0.95, 0.95, 0.95, 0.95, 0.92])
    report_strict = analyse_trend(h, degradation_threshold=0.02)
    report_default = analyse_trend(h, degradation_threshold=0.05)
    assert report_strict.is_degrading is True
    assert report_default.is_degrading is False


def test_summary_line_degrading():
    h = _history_with_rates([0.95, 0.95, 0.95, 0.95, 0.80])
    report = analyse_trend(h)
    line = report.summary_line()
    assert "degrading" in line
    assert "orders" in line


def test_summary_line_recovering():
    h = _history_with_rates([0.70, 0.70, 0.70, 0.70, 0.95])
    report = analyse_trend(h)
    assert "recovering" in report.summary_line()


def test_summary_line_stable():
    h = _history_with_rates([0.95, 0.95, 0.95, 0.95, 0.95])
    report = analyse_trend(h)
    assert "stable" in report.summary_line()
