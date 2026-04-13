"""Tests for pipewatch.tag_filter."""
from __future__ import annotations

import pytest

from pipewatch.metrics import PipelineMetric
from pipewatch.tag_filter import TagFilter, filter_metrics, group_by_tag


def _metric(name: str, tags: dict | None = None) -> PipelineMetric:
    m = PipelineMetric(
        pipeline_name=name,
        rows_processed=100,
        rows_failed=0,
        duration_seconds=1.0,
    )
    m.tags = tags or {}
    return m


# ---------------------------------------------------------------------------
# TagFilter.matches
# ---------------------------------------------------------------------------

def test_empty_filter_matches_any_metric():
    f = TagFilter()
    assert f.matches(_metric("p", {"env": "prod"}))


def test_filter_matches_when_all_tags_present():
    f = TagFilter(required={"env": "prod", "team": "data"})
    assert f.matches(_metric("p", {"env": "prod", "team": "data", "region": "us"}))


def test_filter_rejects_wrong_tag_value():
    f = TagFilter(required={"env": "prod"})
    assert not f.matches(_metric("p", {"env": "staging"}))


def test_filter_rejects_missing_tag():
    f = TagFilter(required={"env": "prod"})
    assert not f.matches(_metric("p", {}))


def test_filter_handles_metric_without_tags_attribute():
    f = TagFilter(required={"env": "prod"})
    m = PipelineMetric(pipeline_name="x", rows_processed=1, rows_failed=0, duration_seconds=1.0)
    # no .tags attribute set
    assert not f.matches(m)


# ---------------------------------------------------------------------------
# filter_metrics
# ---------------------------------------------------------------------------

def test_filter_metrics_returns_matching_subset():
    metrics = [
        _metric("a", {"env": "prod"}),
        _metric("b", {"env": "staging"}),
        _metric("c", {"env": "prod"}),
    ]
    result = filter_metrics(metrics, TagFilter(required={"env": "prod"}))
    assert [m.pipeline_name for m in result] == ["a", "c"]


def test_filter_metrics_empty_input():
    assert filter_metrics([], TagFilter(required={"env": "prod"})) == []


# ---------------------------------------------------------------------------
# group_by_tag
# ---------------------------------------------------------------------------

def test_group_by_tag_separates_values():
    metrics = [
        _metric("a", {"env": "prod"}),
        _metric("b", {"env": "staging"}),
        _metric("c", {"env": "prod"}),
    ]
    groups = group_by_tag(metrics, "env")
    assert sorted(groups["prod"], key=lambda m: m.pipeline_name) == [metrics[0], metrics[2]]
    assert groups["staging"] == [metrics[1]]


def test_group_by_tag_missing_key_goes_to_empty_string_bucket():
    metrics = [_metric("a", {}), _metric("b", {"env": "prod"})]
    groups = group_by_tag(metrics, "env")
    assert groups[""] == [metrics[0]]
    assert groups["prod"] == [metrics[1]]
