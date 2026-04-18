"""Tests for pipewatch.regression and pipewatch.regression_reporter."""
from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from pipewatch.history import PipelineHistory, MetricSnapshot
from pipewatch.regression import detect_regression
from pipewatch.regression_reporter import (
    format_regression_result,
    format_regression_report,
    regression_report_to_json,
)


def _ts(offset: int = 0) -> datetime:
    return datetime(2024, 1, 1, 12, offset, 0, tzinfo=timezone.utc)


def _snap(success_rate: float, i: int = 0) -> MetricSnapshot:
    return MetricSnapshot(
        pipeline="pipe-a",
        timestamp=_ts(i),
        success_rate=success_rate,
        throughput=100.0,
        error_rate=1.0 - success_rate,
        is_healthy=success_rate >= 0.9,
    )


def _history(rates: list[float]) -> PipelineHistory:
    h = PipelineHistory("pipe-a")
    for i, r in enumerate(rates):
        h.add(_snap(r, i))
    return h


def test_insufficient_data_returns_none():
    h = _history([0.95, 0.94, 0.93])  # fewer than baseline_window + 1
    result = detect_regression(h, baseline_window=10)
    assert result is None


def test_no_regression_when_stable():
    rates = [0.95] * 11
    h = _history(rates)
    result = detect_regression(h, baseline_window=10, threshold_pct=0.10)
    assert result is not None
    assert result.regressed is False


def test_regression_detected_on_sudden_drop():
    # baseline mean ~0.95, current drops to 0.50
    rates = [0.95] * 10 + [0.50]
    h = _history(rates)
    result = detect_regression(h, baseline_window=10, threshold_pct=0.10)
    assert result is not None
    assert result.regressed is True
    assert result.pct_change < -0.10


def test_pct_change_is_correct():
    rates = [1.0] * 10 + [0.8]
    h = _history(rates)
    result = detect_regression(h, baseline_window=10, threshold_pct=0.10)
    assert result is not None
    assert abs(result.pct_change - (-0.20)) < 1e-6


def test_result_to_dict_has_expected_keys():
    rates = [0.95] * 11
    h = _history(rates)
    result = detect_regression(h)
    assert result is not None
    d = result.to_dict()
    for key in ("pipeline", "metric", "current_value", "baseline_mean", "pct_change", "regressed"):
        assert key in d


def test_format_result_none_shows_insufficient():
    text = format_regression_result(None)
    assert "insufficient" in text


def test_format_result_regression_shows_warning():
    rates = [0.95] * 10 + [0.50]
    h = _history(rates)
    result = detect_regression(h, baseline_window=10)
    text = format_regression_result(result)
    assert "REGRESSION" in text
    assert "pipe-a" in text


def test_format_result_ok_shows_ok():
    rates = [0.95] * 11
    h = _history(rates)
    result = detect_regression(h, baseline_window=10)
    text = format_regression_result(result)
    assert "OK" in text


def test_format_report_empty():
    text = format_regression_report([])
    assert "No regression" in text


def test_regression_report_to_json_is_valid():
    rates = [0.95] * 11
    h = _history(rates)
    result = detect_regression(h)
    payload = json.loads(regression_report_to_json([result, None]))
    assert isinstance(payload, list)
    assert len(payload) == 2
    assert payload[1] is None
    assert payload[0]["pipeline"] == "pipe-a"
