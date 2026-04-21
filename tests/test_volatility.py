"""Tests for pipewatch.volatility and pipewatch.volatility_reporter."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pipewatch.history import PipelineHistory, MetricSnapshot
from pipewatch.volatility import detect_volatility
from pipewatch.volatility_reporter import (
    format_volatility_result,
    format_volatility_report,
    volatility_report_to_json,
)


def _ts(offset: int = 0) -> datetime:
    return datetime(2024, 1, 1, 12, offset, 0, tzinfo=timezone.utc)


def _snap(success_rate: float, throughput: float = 100.0, offset: int = 0) -> MetricSnapshot:
    return MetricSnapshot(
        pipeline="pipe_a",
        timestamp=_ts(offset),
        success_rate=success_rate,
        throughput=throughput,
        error_count=0,
        row_count=100,
    )


def _history(snaps: list[MetricSnapshot]) -> PipelineHistory:
    h = PipelineHistory(pipeline="pipe_a")
    for s in snaps:
        h.add(s)
    return h


# ---------------------------------------------------------------------------
# detect_volatility
# ---------------------------------------------------------------------------

def test_empty_history_returns_none():
    h = PipelineHistory(pipeline="pipe_a")
    assert detect_volatility(h) is None


def test_insufficient_data_sets_flag():
    h = _history([_snap(0.9, offset=i) for i in range(3)])  # < 4 samples
    result = detect_volatility(h)
    assert result is not None
    assert result.insufficient_data is True
    assert result.is_volatile is False


def test_stable_series_is_not_volatile():
    # All identical → std_dev = 0, CV = 0
    h = _history([_snap(0.95, offset=i) for i in range(10)])
    result = detect_volatility(h)
    assert result is not None
    assert result.insufficient_data is False
    assert result.is_volatile is False
    assert result.std_dev == pytest.approx(0.0)


def test_highly_variable_series_is_volatile():
    rates = [0.99, 0.10, 0.98, 0.11, 0.97, 0.12, 0.96, 0.13]
    h = _history([_snap(r, offset=i) for i, r in enumerate(rates)])
    result = detect_volatility(h, threshold=0.15)
    assert result is not None
    assert result.is_volatile is True


def test_custom_threshold_changes_verdict():
    rates = [0.90, 0.85, 0.88, 0.87, 0.89]  # small variation
    h = _history([_snap(r, offset=i) for i, r in enumerate(rates)])
    result_strict = detect_volatility(h, threshold=0.01)
    result_lenient = detect_volatility(h, threshold=0.50)
    assert result_strict is not None and result_strict.is_volatile is True
    assert result_lenient is not None and result_lenient.is_volatile is False


def test_throughput_metric_uses_throughput_values():
    snaps = [_snap(0.99, throughput=float(t), offset=i)
             for i, t in enumerate([10, 200, 10, 200, 10, 200, 10, 200])]
    h = _history(snaps)
    result = detect_volatility(h, metric="throughput", threshold=0.15)
    assert result is not None
    assert result.metric == "throughput"
    assert result.is_volatile is True


def test_to_dict_keys_present():
    h = _history([_snap(0.9, offset=i) for i in range(6)])
    result = detect_volatility(h)
    assert result is not None
    d = result.to_dict()
    for key in ("pipeline", "metric", "sample_count", "mean", "std_dev",
                "coefficient_of_variation", "is_volatile", "insufficient_data", "threshold"):
        assert key in d


# ---------------------------------------------------------------------------
# reporter
# ---------------------------------------------------------------------------

def test_format_result_stable_contains_checkmark():
    h = _history([_snap(0.95, offset=i) for i in range(6)])
    result = detect_volatility(h)
    assert result is not None
    text = format_volatility_result(result)
    assert "✅" in text
    assert "stable" in text


def test_format_result_volatile_contains_fire():
    rates = [0.99, 0.10, 0.98, 0.11, 0.97, 0.12, 0.96, 0.13]
    h = _history([_snap(r, offset=i) for i, r in enumerate(rates)])
    result = detect_volatility(h, threshold=0.15)
    assert result is not None
    text = format_volatility_result(result)
    assert "🔥" in text
    assert "VOLATILE" in text


def test_format_report_empty_shows_message():
    assert "No volatility" in format_volatility_report([])


def test_format_report_contains_header():
    h = _history([_snap(0.95, offset=i) for i in range(6)])
    result = detect_volatility(h)
    assert result is not None
    text = format_volatility_report([result])
    assert "Volatility Report" in text


def test_json_output_is_list():
    import json as _json
    h = _history([_snap(0.9, offset=i) for i in range(6)])
    result = detect_volatility(h)
    assert result is not None
    data = _json.loads(volatility_report_to_json([result]))
    assert isinstance(data, list)
    assert len(data) == 1
