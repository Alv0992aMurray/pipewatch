"""Tests for pipewatch.normalization."""
import pytest
from pipewatch.metrics import PipelineMetric
from pipewatch.normalization import (
    NormalizationConfig,
    normalize_metric,
    normalize_metrics,
)


def _metric(
    pipeline="pipe",
    rows_processed=100,
    rows_passed=90,
    rows_failed=10,
    duration_seconds=10.0,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline_name=pipeline,
        rows_processed=rows_processed,
        rows_passed=rows_passed,
        rows_failed=rows_failed,
        duration_seconds=duration_seconds,
    )


def test_perfect_metric_success_rate_is_one():
    m = _metric(rows_processed=100, rows_passed=100, rows_failed=0)
    result = normalize_metric(m)
    assert result.success_rate == pytest.approx(1.0)


def test_zero_success_rate_normalizes_to_zero():
    m = _metric(rows_processed=100, rows_passed=0, rows_failed=100)
    result = normalize_metric(m)
    assert result.success_rate == pytest.approx(0.0)


def test_partial_success_rate_is_between_zero_and_one():
    m = _metric(rows_processed=100, rows_passed=50, rows_failed=50)
    result = normalize_metric(m)
    assert 0.0 < result.success_rate < 1.0


def test_throughput_is_normalized():
    cfg = NormalizationConfig(throughput_ceiling=100.0)
    m = _metric(rows_processed=50, duration_seconds=1.0)
    result = normalize_metric(m, cfg)
    assert result.throughput == pytest.approx(0.5)


def test_throughput_above_ceiling_clamped_to_one():
    cfg = NormalizationConfig(throughput_ceiling=10.0)
    m = _metric(rows_processed=1000, duration_seconds=1.0)
    result = normalize_metric(m, cfg)
    assert result.throughput == pytest.approx(1.0)


def test_no_rows_returns_none_for_rates():
    m = _metric(rows_processed=0, rows_passed=0, rows_failed=0)
    result = normalize_metric(m)
    assert result.success_rate is None
    assert result.error_rate is None


def test_no_duration_returns_none_for_throughput():
    m = PipelineMetric(
        pipeline_name="p",
        rows_processed=100,
        rows_passed=90,
        rows_failed=10,
        duration_seconds=None,
    )
    result = normalize_metric(m)
    assert result.throughput is None


def test_to_dict_has_expected_keys():
    m = _metric()
    result = normalize_metric(m)
    d = result.to_dict()
    assert set(d.keys()) == {"pipeline", "success_rate", "throughput", "error_rate"}


def test_normalize_metrics_returns_one_per_input():
    metrics = [_metric(pipeline=f"p{i}") for i in range(4)]
    results = normalize_metrics(metrics)
    assert len(results) == 4
    assert [r.pipeline for r in results] == ["p0", "p1", "p2", "p3"]
