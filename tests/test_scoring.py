"""Tests for pipewatch.scoring."""
import pytest

from pipewatch.metrics import PipelineMetric
from pipewatch.scoring import (
    ScoringWeights,
    ScoreResult,
    _grade,
    _normalise_throughput,
    score_metric,
)


@pytest.fixture
def healthy_metric():
    return PipelineMetric(
        pipeline_name="orders",
        total_rows=1000,
        failed_rows=10,
        duration_seconds=100.0,
    )


@pytest.fixture
def failing_metric():
    return PipelineMetric(
        pipeline_name="returns",
        total_rows=100,
        failed_rows=90,
        duration_seconds=200.0,
    )


def test_grade_boundaries():
    assert _grade(95) == "A"
    assert _grade(90) == "A"
    assert _grade(89) == "B"
    assert _grade(75) == "B"
    assert _grade(74) == "C"
    assert _grade(60) == "C"
    assert _grade(59) == "D"
    assert _grade(40) == "D"
    assert _grade(39) == "F"
    assert _grade(0) == "F"


def test_normalise_throughput_caps_at_one():
    assert _normalise_throughput(20_000, ceiling=10_000) == 1.0


def test_normalise_throughput_zero_ceiling_returns_zero():
    assert _normalise_throughput(500, ceiling=0) == 0.0


def test_normalise_throughput_proportional():
    result = _normalise_throughput(5_000, ceiling=10_000)
    assert abs(result - 0.5) < 1e-9


def test_score_healthy_metric_is_high(healthy_metric):
    result = score_metric(healthy_metric)
    assert isinstance(result, ScoreResult)
    assert result.score > 75
    assert result.grade in ("A", "B")
    assert result.pipeline == "orders"


def test_score_failing_metric_is_low(failing_metric):
    result = score_metric(failing_metric)
    assert result.score < 50
    assert result.grade in ("D", "F")


def test_score_breakdown_keys(healthy_metric):
    result = score_metric(healthy_metric)
    assert set(result.breakdown.keys()) == {"success_rate", "throughput", "error_rate"}


def test_score_breakdown_sums_to_total(healthy_metric):
    result = score_metric(healthy_metric)
    total = sum(result.breakdown.values())
    assert abs(total - result.score) < 1e-6


def test_custom_weights(healthy_metric):
    weights = ScoringWeights(success_rate=0.8, throughput=0.1, error_rate=0.1)
    result = score_metric(healthy_metric, weights=weights)
    assert result.score > 0


def test_invalid_weights_raises(healthy_metric):
    bad = ScoringWeights(success_rate=0.5, throughput=0.5, error_rate=0.5)
    with pytest.raises(ValueError, match="Weights must sum to 1.0"):
        score_metric(healthy_metric, weights=bad)


def test_to_dict_contains_expected_keys(healthy_metric):
    result = score_metric(healthy_metric)
    d = result.to_dict()
    assert "pipeline" in d
    assert "score" in d
    assert "grade" in d
    assert "breakdown" in d


def test_zero_rows_scores_low():
    metric = PipelineMetric(pipeline_name="empty", total_rows=0, failed_rows=0, duration_seconds=1.0)
    result = score_metric(metric)
    assert result.score < 50
