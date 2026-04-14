"""Tests for pipewatch.aggregation and pipewatch.aggregation_reporter."""
from __future__ import annotations

import json

import pytest

from pipewatch.metrics import PipelineMetric
from pipewatch.aggregation import AggregationGroup, aggregate_by_tag
from pipewatch.aggregation_reporter import (
    format_group,
    format_aggregation_report,
    aggregation_report_to_json,
)


def _metric(name: str, processed: int, failed: int, elapsed: float = 10.0, tags: dict | None = None) -> PipelineMetric:
    return PipelineMetric(
        pipeline_name=name,
        rows_processed=processed,
        rows_failed=failed,
        elapsed_seconds=elapsed,
        tags=tags or {},
    )


@pytest.fixture
def healthy_metric():
    return _metric("pipe_a", 100, 0, tags={"env": "prod"})


@pytest.fixture
def failing_metric():
    return _metric("pipe_b", 50, 40, tags={"env": "staging"})


# --- AggregationGroup ---------------------------------------------------------

def test_empty_group_has_zero_count():
    g = AggregationGroup(name="empty")
    assert g.count == 0
    assert g.healthy_count == 0
    assert g.health_ratio is None
    assert g.avg_success_rate is None
    assert g.avg_throughput is None


def test_single_healthy_metric(healthy_metric):
    g = AggregationGroup(name="prod", metrics=[healthy_metric])
    assert g.count == 1
    assert g.healthy_count == 1
    assert g.unhealthy_count == 0
    assert g.health_ratio == pytest.approx(1.0)
    assert g.avg_success_rate == pytest.approx(1.0)


def test_mixed_metrics(healthy_metric, failing_metric):
    g = AggregationGroup(name="mixed", metrics=[healthy_metric, failing_metric])
    assert g.count == 2
    assert g.healthy_count == 1
    assert g.unhealthy_count == 1
    assert g.health_ratio == pytest.approx(0.5)


def test_avg_success_rate_is_mean_of_individual_rates(healthy_metric, failing_metric):
    g = AggregationGroup(name="mixed", metrics=[healthy_metric, failing_metric])
    # healthy SR = 1.0, failing SR = 10/50 = 0.2  => mean = 0.6
    assert g.avg_success_rate == pytest.approx(0.6)


def test_avg_throughput_computed_correctly():
    m1 = _metric("a", 100, 0, elapsed=10.0)   # 10 rows/s
    m2 = _metric("b", 200, 0, elapsed=10.0)   # 20 rows/s
    g = AggregationGroup(name="g", metrics=[m1, m2])
    assert g.avg_throughput == pytest.approx(15.0)


def test_to_dict_keys(healthy_metric):
    g = AggregationGroup(name="prod", metrics=[healthy_metric])
    d = g.to_dict()
    assert set(d.keys()) == {"group", "count", "healthy", "unhealthy", "health_ratio", "avg_success_rate", "avg_throughput"}


# --- aggregate_by_tag ---------------------------------------------------------

def test_aggregate_by_tag_creates_correct_groups():
    metrics = [
        _metric("a", 100, 0, tags={"env": "prod"}),
        _metric("b", 100, 0, tags={"env": "prod"}),
        _metric("c", 50, 40, tags={"env": "staging"}),
    ]
    groups = aggregate_by_tag(metrics, "env")
    names = {g.name for g in groups}
    assert names == {"prod", "staging"}
    prod_group = next(g for g in groups if g.name == "prod")
    assert prod_group.count == 2


def test_aggregate_by_tag_untagged_metrics():
    m = _metric("x", 100, 0, tags={})
    groups = aggregate_by_tag([m], "env")
    assert len(groups) == 1
    assert groups[0].name == "__untagged__"


# --- reporter -----------------------------------------------------------------

def test_format_group_contains_name(healthy_metric):
    g = AggregationGroup(name="prod", metrics=[healthy_metric])
    out = format_group(g)
    assert "prod" in out
    assert "100.0%" in out  # health ratio


def test_format_aggregation_report_empty():
    assert format_aggregation_report([]) == "No aggregation groups."


def test_aggregation_report_to_json(healthy_metric, failing_metric):
    g = AggregationGroup(name="all", metrics=[healthy_metric, failing_metric])
    payload = json.loads(aggregation_report_to_json([g]))
    assert isinstance(payload, list)
    assert payload[0]["group"] == "all"
    assert payload[0]["count"] == 2
