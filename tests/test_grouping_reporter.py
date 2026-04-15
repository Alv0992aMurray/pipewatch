"""Tests for pipewatch.grouping_reporter."""
from __future__ import annotations

import json

import pytest

from pipewatch.metrics import PipelineMetric
from pipewatch.grouping import MetricGroup, group_by_pipeline
from pipewatch.grouping_reporter import (
    format_group,
    format_grouping_report,
    grouping_report_to_json,
)


def _metric(name: str = "pipe", failed: int = 0) -> PipelineMetric:
    return PipelineMetric(
        pipeline_name=name,
        total_rows=100,
        processed_rows=100 - failed,
        failed_rows=failed,
        duration_seconds=1.0,
        tags={},
    )


@pytest.fixture()
def healthy_group() -> MetricGroup:
    return MetricGroup(key="prod", metrics=[_metric("a"), _metric("b")])


@pytest.fixture()
def degraded_group() -> MetricGroup:
    return MetricGroup(key="staging", metrics=[_metric("c", failed=40)])


def test_format_group_healthy_shows_ok(healthy_group):
    line = format_group(healthy_group)
    assert "OK" in line
    assert "prod" in line


def test_format_group_degraded_shows_degraded(degraded_group):
    line = format_group(degraded_group)
    assert "DEGRADED" in line


def test_format_group_contains_counts(healthy_group):
    line = format_group(healthy_group)
    assert "2 pipeline(s)" in line
    assert "2 healthy" in line
    assert "0 unhealthy" in line


def test_format_group_shows_avg_success_rate(healthy_group):
    line = format_group(healthy_group)
    assert "100.0%" in line


def test_format_grouping_report_empty_returns_message():
    report = format_grouping_report({})
    assert "No groups" in report


def test_format_grouping_report_includes_all_groups(healthy_group, degraded_group):
    groups = {"prod": healthy_group, "staging": degraded_group}
    report = format_grouping_report(groups)
    assert "prod" in report
    assert "staging" in report
    assert "Pipeline Grouping Report" in report


def test_grouping_report_to_json_is_valid_json(healthy_group, degraded_group):
    groups = {"prod": healthy_group, "staging": degraded_group}
    raw = grouping_report_to_json(groups)
    data = json.loads(raw)
    assert isinstance(data, list)
    assert len(data) == 2


def test_grouping_report_to_json_sorted_by_key(healthy_group, degraded_group):
    groups = {"prod": healthy_group, "staging": degraded_group}
    data = json.loads(grouping_report_to_json(groups))
    keys = [entry["key"] for entry in data]
    assert keys == sorted(keys)
