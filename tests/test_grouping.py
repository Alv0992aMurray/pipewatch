"""Tests for pipewatch.grouping."""
from __future__ import annotations

import pytest

from pipewatch.metrics import PipelineMetric
from pipewatch.grouping import (
    MetricGroup,
    group_metrics,
    group_by_pipeline,
    group_by_tag_value,
)


def _metric(
    name: str = "pipe",
    processed: int = 100,
    failed: int = 0,
    tags: dict | None = None,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline_name=name,
        total_rows=processed + failed,
        processed_rows=processed,
        failed_rows=failed,
        duration_seconds=1.0,
        tags=tags or {},
    )


# ---------------------------------------------------------------------------
# MetricGroup unit tests
# ---------------------------------------------------------------------------

def test_empty_group_has_zero_count():
    g = MetricGroup(key="test")
    assert g.count() == 0
    assert g.healthy_count() == 0
    assert g.unhealthy_count() == 0
    assert g.avg_success_rate() is None


def test_group_counts_healthy_and_unhealthy():
    g = MetricGroup(key="k", metrics=[_metric(failed=0), _metric(failed=50)])
    assert g.count() == 2
    assert g.healthy_count() == 1
    assert g.unhealthy_count() == 1


def test_group_avg_success_rate():
    m1 = _metric(processed=90, failed=10)   # 90 %
    m2 = _metric(processed=80, failed=20)   # 80 %
    g = MetricGroup(key="k", metrics=[m1, m2])
    assert abs(g.avg_success_rate() - 0.85) < 1e-9


def test_group_to_dict_keys():
    g = MetricGroup(key="env:prod", metrics=[_metric()])
    d = g.to_dict()
    assert set(d.keys()) == {"key", "count", "healthy", "unhealthy", "avg_success_rate"}


# ---------------------------------------------------------------------------
# group_metrics
# ---------------------------------------------------------------------------

def test_group_metrics_partitions_correctly():
    m1 = _metric(name="a")
    m2 = _metric(name="b")
    m3 = _metric(name="a")
    groups = group_metrics([m1, m2, m3], lambda m: m.pipeline_name)
    assert set(groups.keys()) == {"a", "b"}
    assert groups["a"].count() == 2
    assert groups["b"].count() == 1


def test_group_metrics_none_key_uses_default():
    m = _metric()
    groups = group_metrics([m], lambda _: None, default_key="other")
    assert "other" in groups


# ---------------------------------------------------------------------------
# group_by_pipeline
# ---------------------------------------------------------------------------

def test_group_by_pipeline_creates_one_group_per_pipeline():
    metrics = [_metric("pipe_a"), _metric("pipe_b"), _metric("pipe_a")]
    groups = group_by_pipeline(metrics)
    assert len(groups) == 2
    assert groups["pipe_a"].count() == 2


# ---------------------------------------------------------------------------
# group_by_tag_value
# ---------------------------------------------------------------------------

def test_group_by_tag_value_splits_on_tag():
    m_prod = _metric(tags={"env": "prod"})
    m_stg = _metric(tags={"env": "staging"})
    m_none = _metric(tags={})
    groups = group_by_tag_value([m_prod, m_stg, m_none], "env")
    assert "prod" in groups
    assert "staging" in groups
    assert "(ungrouped)" in groups


def test_group_by_tag_value_missing_tag_goes_to_ungrouped():
    m = _metric(tags={"other": "value"})
    groups = group_by_tag_value([m], "env")
    assert "(ungrouped)" in groups
    assert groups["(ungrouped)"].count() == 1
