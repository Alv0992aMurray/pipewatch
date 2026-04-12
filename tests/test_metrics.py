"""Unit tests for pipewatch.metrics module."""

from datetime import datetime

import pytest

from pipewatch.metrics import PipelineMetric


@pytest.fixture
def healthy_metric():
    return PipelineMetric(
        pipeline_name="orders_etl",
        rows_processed=980,
        rows_failed=20,
        duration_seconds=10.0,
        stage="transform",
    )


@pytest.fixture
def failing_metric():
    return PipelineMetric(
        pipeline_name="inventory_etl",
        rows_processed=500,
        rows_failed=600,
        duration_seconds=5.0,
    )


def test_success_rate_healthy(healthy_metric):
    assert healthy_metric.success_rate == 98.0


def test_success_rate_failing(failing_metric):
    assert failing_metric.success_rate == pytest.approx(45.45, rel=1e-2)


def test_success_rate_no_rows():
    metric = PipelineMetric("empty_pipeline", 0, 0, 1.0)
    assert metric.success_rate == 100.0


def test_throughput(healthy_metric):
    assert healthy_metric.throughput == 98.0


def test_throughput_zero_duration():
    metric = PipelineMetric("zero_dur", 100, 0, 0.0)
    assert metric.throughput == 0.0


def test_is_healthy_above_threshold(healthy_metric):
    assert healthy_metric.is_healthy() is True


def test_is_healthy_below_threshold(failing_metric):
    assert failing_metric.is_healthy() is False


def test_is_healthy_custom_threshold(healthy_metric):
    assert healthy_metric.is_healthy(min_success_rate=99.0) is False


def test_to_dict_keys(healthy_metric):
    result = healthy_metric.to_dict()
    expected_keys = {
        "pipeline_name", "stage", "rows_processed", "rows_failed",
        "duration_seconds", "success_rate", "throughput", "timestamp", "healthy",
    }
    assert expected_keys == set(result.keys())


def test_to_dict_timestamp_is_iso_string(healthy_metric):
    result = healthy_metric.to_dict()
    # Should not raise
    datetime.fromisoformat(result["timestamp"])


def test_default_timestamp_set():
    before = datetime.utcnow()
    metric = PipelineMetric("ts_test", 10, 0, 1.0)
    after = datetime.utcnow()
    assert before <= metric.timestamp <= after
