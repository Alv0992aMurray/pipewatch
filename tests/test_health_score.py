"""Tests for pipewatch.health_score."""
import pytest
from pipewatch.metrics import PipelineMetric
from pipewatch.health_score import (
    score_metric,
    build_health_score_report,
    _grade,
    HealthScoreReport,
)


@pytest.fixture
def healthy_metric():
    return PipelineMetric(
        pipeline="orders",
        rows_processed=900,
        rows_failed=10,
        duration_seconds=90.0,
    )


@pytest.fixture
def failing_metric():
    return PipelineMetric(
        pipeline="payments",
        rows_processed=100,
        rows_failed=80,
        duration_seconds=120.0,
    )


def test_grade_boundaries():
    assert _grade(95) == "A"
    assert _grade(80) == "B"
    assert _grade(65) == "C"
    assert _grade(50) == "D"
    assert _grade(30) == "F"


def test_score_healthy_metric_is_high(healthy_metric):
    result = score_metric(healthy_metric)
    assert result.score > 60
    assert result.pipeline == "orders"
    assert result.is_healthy is True


def test_score_failing_metric_is_low(failing_metric):
    result = score_metric(failing_metric)
    assert result.score < 50
    assert result.is_healthy is False


def test_score_to_dict_has_expected_keys(healthy_metric):
    result = score_metric(healthy_metric)
    d = result.to_dict()
    assert "pipeline" in d
    assert "score" in d
    assert "grade" in d
    assert "success_rate" in d
    assert "throughput" in d
    assert "is_healthy" in d


def test_build_report_empty():
    report = build_health_score_report([])
    assert report.scores == []
    assert report.average_score is None
    assert report.healthy_count == 0
    assert report.unhealthy_count == 0


def test_build_report_counts(healthy_metric, failing_metric):
    report = build_health_score_report([healthy_metric, failing_metric])
    assert len(report.scores) == 2
    assert report.healthy_count == 1
    assert report.unhealthy_count == 1


def test_build_report_average_score(healthy_metric, failing_metric):
    report = build_health_score_report([healthy_metric, failing_metric])
    avg = report.average_score
    assert avg is not None
    assert 0 <= avg <= 100


def test_throughput_ceiling_zero_does_not_crash(healthy_metric):
    result = score_metric(healthy_metric, throughput_ceiling=0.0)
    assert 0 <= result.score <= 100
