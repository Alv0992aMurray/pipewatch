"""Tests for pipewatch.percentile."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pipewatch.history import MetricSnapshot, PipelineHistory
from pipewatch.percentile import MIN_SAMPLES, PercentileResult, compute_percentiles


def _ts(offset: int = 0) -> datetime:
    return datetime(2024, 1, 1, 12, offset, 0, tzinfo=timezone.utc)


def _snap(pipeline: str, success_rate: float, offset: int = 0) -> MetricSnapshot:
    return MetricSnapshot(
        pipeline=pipeline,
        timestamp=_ts(offset),
        data={"success_rate": success_rate, "throughput": 100.0},
    )


def _history(rates: list[float], pipeline: str = "pipe_a") -> PipelineHistory:
    h = PipelineHistory(pipeline)
    for i, r in enumerate(rates):
        h.add(_snap(pipeline, r, offset=i))
    return h


# ---------------------------------------------------------------------------
# edge cases
# ---------------------------------------------------------------------------

def test_empty_history_returns_none():
    h = PipelineHistory("pipe_a")
    assert compute_percentiles(h) is None


def test_insufficient_data_sets_flag():
    h = _history([0.9, 0.8, 0.7])  # fewer than MIN_SAMPLES
    result = compute_percentiles(h)
    assert result is not None
    assert result.insufficient_data is True
    assert result.p50 is None
    assert result.p90 is None


def test_exactly_min_samples_does_not_set_flag():
    rates = [0.9] * MIN_SAMPLES
    h = _history(rates)
    result = compute_percentiles(h)
    assert result is not None
    assert result.insufficient_data is False


# ---------------------------------------------------------------------------
# correctness
# ---------------------------------------------------------------------------

def test_p50_is_median():
    # sorted: [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    rates = [0.5, 0.1, 0.9, 0.3, 0.7, 0.2, 0.8, 0.4, 0.6, 1.0]
    h = _history(rates)
    result = compute_percentiles(h)
    assert result is not None
    assert result.p50 == pytest.approx(0.5, abs=0.05)


def test_p99_is_near_max_for_large_sample():
    rates = [float(i) / 100.0 for i in range(1, 101)]  # 0.01 .. 1.0
    h = _history(rates)
    result = compute_percentiles(h)
    assert result is not None
    assert result.p99 >= 0.98


def test_uniform_series_all_percentiles_equal():
    rates = [0.95] * 10
    h = _history(rates)
    result = compute_percentiles(h)
    assert result is not None
    assert result.p50 == pytest.approx(0.95)
    assert result.p90 == pytest.approx(0.95)
    assert result.p95 == pytest.approx(0.95)
    assert result.p99 == pytest.approx(0.95)


def test_sample_count_matches_history_length():
    rates = [0.8, 0.85, 0.9, 0.75, 0.7, 0.95, 0.6]
    h = _history(rates)
    result = compute_percentiles(h)
    assert result is not None
    assert result.sample_count == len(rates)


def test_pipeline_name_propagated():
    h = _history([0.9] * 6, pipeline="etl_orders")
    result = compute_percentiles(h)
    assert result is not None
    assert result.pipeline == "etl_orders"


def test_custom_metric_used():
    h = PipelineHistory("pipe_b")
    for i in range(6):
        snap = MetricSnapshot(
            pipeline="pipe_b",
            timestamp=_ts(i),
            data={"success_rate": 0.5, "throughput": float(100 + i * 10)},
        )
        h.add(snap)
    result = compute_percentiles(h, metric="throughput")
    assert result is not None
    assert result.metric == "throughput"
    assert result.p50 is not None


def test_to_dict_contains_expected_keys():
    h = _history([0.9] * 6)
    result = compute_percentiles(h)
    assert result is not None
    d = result.to_dict()
    for key in ("pipeline", "metric", "sample_count", "p50", "p90", "p95", "p99", "insufficient_data"):
        assert key in d
