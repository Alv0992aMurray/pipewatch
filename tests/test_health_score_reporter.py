"""Tests for pipewatch.health_score_reporter."""
import json
import pytest
from pipewatch.metrics import PipelineMetric
from pipewatch.health_score import build_health_score_report, score_metric
from pipewatch.health_score_reporter import (
    format_pipeline_score,
    format_health_score_report,
    health_score_report_to_json,
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


def test_format_pipeline_score_contains_pipeline_name(healthy_metric):
    entry = score_metric(healthy_metric)
    line = format_pipeline_score(entry)
    assert "orders" in line


def test_format_pipeline_score_shows_ok_for_healthy(healthy_metric):
    entry = score_metric(healthy_metric)
    line = format_pipeline_score(entry)
    assert "OK" in line


def test_format_pipeline_score_shows_fail_for_failing(failing_metric):
    entry = score_metric(failing_metric)
    line = format_pipeline_score(entry)
    assert "FAIL" in line


def test_format_report_empty():
    report = build_health_score_report([])
    text = format_health_score_report(report)
    assert "no pipelines" in text


def test_format_report_contains_header(healthy_metric):
    report = build_health_score_report([healthy_metric])
    text = format_health_score_report(report)
    assert "Health Score Report" in text


def test_format_report_contains_pipeline(healthy_metric, failing_metric):
    report = build_health_score_report([healthy_metric, failing_metric])
    text = format_health_score_report(report)
    assert "orders" in text
    assert "payments" in text


def test_json_output_is_valid(healthy_metric, failing_metric):
    report = build_health_score_report([healthy_metric, failing_metric])
    raw = health_score_report_to_json(report)
    data = json.loads(raw)
    assert "pipelines" in data
    assert "summary" in data
    assert data["summary"]["total"] == 2


def test_json_summary_counts(healthy_metric, failing_metric):
    report = build_health_score_report([healthy_metric, failing_metric])
    data = json.loads(health_score_report_to_json(report))
    assert data["summary"]["healthy"] == 1
    assert data["summary"]["unhealthy"] == 1


def test_json_empty_report():
    report = build_health_score_report([])
    data = json.loads(health_score_report_to_json(report))
    assert data["summary"]["average_score"] is None
    assert data["pipelines"] == []
