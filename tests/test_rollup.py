"""Tests for pipewatch.rollup and pipewatch.rollup_reporter."""
import json
import pytest

from pipewatch.metrics import PipelineMetric
from pipewatch.rollup import compute_rollup, RollupStats
from pipewatch.rollup_reporter import format_rollup, rollup_to_json


@pytest.fixture
def healthy_metric():
    return PipelineMetric(
        pipeline_id="pipe-a",
        rows_processed=1000,
        rows_failed=10,
        duration_seconds=100.0,
    )


@pytest.fixture
def failing_metric():
    return PipelineMetric(
        pipeline_id="pipe-b",
        rows_processed=500,
        rows_failed=400,
        duration_seconds=50.0,
    )


def test_rollup_empty_list():
    stats = compute_rollup([])
    assert stats.pipeline_count == 0
    assert stats.total_rows_processed == 0
    assert stats.total_errors == 0
    assert stats.avg_success_rate is None
    assert stats.min_success_rate is None
    assert stats.max_success_rate is None
    assert stats.avg_throughput is None
    assert stats.healthy_count == 0
    assert stats.unhealthy_count == 0


def test_rollup_single_healthy(healthy_metric):
    stats = compute_rollup([healthy_metric])
    assert stats.pipeline_count == 1
    assert stats.total_rows_processed == 1000
    assert stats.total_errors == 10
    assert stats.healthy_count == 1
    assert stats.unhealthy_count == 0
    assert stats.avg_success_rate == pytest.approx(0.99)
    assert stats.min_success_rate == pytest.approx(0.99)
    assert stats.max_success_rate == pytest.approx(0.99)


def test_rollup_mixed_metrics(healthy_metric, failing_metric):
    stats = compute_rollup([healthy_metric, failing_metric])
    assert stats.pipeline_count == 2
    assert stats.total_rows_processed == 1500
    assert stats.total_errors == 410
    assert stats.healthy_count == 1
    assert stats.unhealthy_count == 1
    assert stats.min_success_rate == pytest.approx(0.20)
    assert stats.max_success_rate == pytest.approx(0.99)
    expected_avg = (0.99 + 0.20) / 2
    assert stats.avg_success_rate == pytest.approx(expected_avg)


def test_rollup_throughput(healthy_metric):
    stats = compute_rollup([healthy_metric])
    # 1000 rows / 100 s = 10 rows/s
    assert stats.avg_throughput == pytest.approx(10.0)


def test_format_rollup_contains_key_info(healthy_metric, failing_metric):
    stats = compute_rollup([healthy_metric, failing_metric])
    report = format_rollup(stats)
    assert "Pipeline Rollup Summary" in report
    assert "Pipelines" in report
    assert "Healthy" in report
    assert "Success Rate" in report
    assert "Throughput" in report


def test_format_rollup_empty():
    stats = compute_rollup([])
    report = format_rollup(stats)
    assert "n/a" in report


def test_rollup_to_json(healthy_metric):
    stats = compute_rollup([healthy_metric])
    data = json.loads(rollup_to_json(stats))
    assert data["pipeline_count"] == 1
    assert data["total_rows_processed"] == 1000
    assert "avg_success_rate" in data
