"""Tests for pipewatch.labeling."""
from __future__ import annotations

import pytest

from pipewatch.metrics import PipelineMetric
from pipewatch.labeling import LabelRule, LabelResult, apply_labels, label_metrics


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def healthy_metric() -> PipelineMetric:
    return PipelineMetric(
        pipeline_name="orders",
        rows_processed=1000,
        rows_failed=10,
        duration_seconds=30.0,
        tags={"env": "prod", "team": "data"},
    )


@pytest.fixture()
def failing_metric() -> PipelineMetric:
    return PipelineMetric(
        pipeline_name="payments",
        rows_processed=500,
        rows_failed=400,
        duration_seconds=60.0,
        tags={"env": "prod", "team": "finance"},
    )


@pytest.fixture()
def degraded_rule() -> LabelRule:
    """Label 'degraded' for prod pipelines with success rate <= 0.5."""
    return LabelRule(
        label="degraded",
        tag_key="env",
        tag_value="prod",
        max_success_rate=0.5,
    )


@pytest.fixture()
def healthy_rule() -> LabelRule:
    """Label 'healthy' for prod pipelines with success rate >= 0.95."""
    return LabelRule(
        label="healthy",
        tag_key="env",
        tag_value="prod",
        min_success_rate=0.95,
    )


# ---------------------------------------------------------------------------
# LabelRule.matches
# ---------------------------------------------------------------------------

def test_rule_matches_when_tag_and_rate_satisfied(healthy_metric, healthy_rule):
    assert healthy_rule.matches(healthy_metric) is True


def test_rule_rejects_wrong_tag_value(healthy_metric, healthy_rule):
    healthy_metric.tags["env"] = "staging"
    assert healthy_rule.matches(healthy_metric) is False


def test_rule_rejects_missing_tag(healthy_metric, healthy_rule):
    del healthy_metric.tags["env"]
    assert healthy_rule.matches(healthy_metric) is False


def test_rule_rejects_rate_below_min(failing_metric, healthy_rule):
    # failing_metric has rate ~0.2, well below 0.95
    failing_metric.tags["env"] = "prod"
    assert healthy_rule.matches(failing_metric) is False


def test_degraded_rule_matches_failing_metric(failing_metric, degraded_rule):
    assert degraded_rule.matches(failing_metric) is True


def test_degraded_rule_rejects_healthy_metric(healthy_metric, degraded_rule):
    # healthy_metric rate ~0.99, above max_success_rate 0.5
    assert degraded_rule.matches(healthy_metric) is False


# ---------------------------------------------------------------------------
# apply_labels
# ---------------------------------------------------------------------------

def test_apply_labels_assigns_matching_labels(healthy_metric, healthy_rule, degraded_rule):
    result = apply_labels(healthy_metric, [healthy_rule, degraded_rule])
    assert result.pipeline == "orders"
    assert "healthy" in result.labels
    assert "degraded" not in result.labels


def test_apply_labels_no_match_returns_empty_labels(failing_metric, healthy_rule):
    failing_metric.tags["env"] = "staging"
    result = apply_labels(failing_metric, [healthy_rule])
    assert result.labels == []


def test_apply_labels_deduplicates_same_label(healthy_metric, healthy_rule):
    result = apply_labels(healthy_metric, [healthy_rule, healthy_rule])
    assert result.labels.count("healthy") == 1


# ---------------------------------------------------------------------------
# label_metrics
# ---------------------------------------------------------------------------

def test_label_metrics_returns_one_result_per_metric(
    healthy_metric, failing_metric, healthy_rule, degraded_rule
):
    results = label_metrics([healthy_metric, failing_metric], [healthy_rule, degraded_rule])
    assert len(results) == 2


def test_label_metrics_to_dict_contains_expected_keys(healthy_metric, healthy_rule):
    results = label_metrics([healthy_metric], [healthy_rule])
    d = results[0].to_dict()
    assert "pipeline" in d
    assert "labels" in d
    assert isinstance(d["labels"], list)
