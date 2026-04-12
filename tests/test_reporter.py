"""Tests for pipewatch.reporter module."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from pipewatch.alerts import Alert, AlertSeverity
from pipewatch.metrics import PipelineMetric
from pipewatch.reporter import (
    build_report,
    format_alerts,
    format_metric_summary,
    report_to_json,
)


@pytest.fixture
def healthy_metric():
    return PipelineMetric(
        pipeline_name="etl_orders",
        rows_in=1000,
        rows_out=995,
        error_count=0,
        duration_seconds=2.5,
    )


@pytest.fixture
def failing_metric():
    return PipelineMetric(
        pipeline_name="etl_orders",
        rows_in=1000,
        rows_out=500,
        error_count=50,
        duration_seconds=10.0,
    )


@pytest.fixture
def sample_alert():
    return Alert(
        rule_name="low_success_rate",
        severity=AlertSeverity.CRITICAL,
        message="Success rate below threshold",
    )


def test_format_metric_summary_healthy(healthy_metric):
    summary = format_metric_summary(healthy_metric)
    assert "HEALTHY" in summary
    assert "etl_orders" in summary
    assert "rows_in=1000" in summary


def test_format_metric_summary_unhealthy(failing_metric):
    summary = format_metric_summary(failing_metric)
    assert "UNHEALTHY" in summary


def test_format_alerts_empty():
    result = format_alerts([])
    assert result == "No alerts triggered."


def test_format_alerts_with_alert(sample_alert):
    result = format_alerts([sample_alert])
    assert "CRITICAL" in result
    assert "low_success_rate" in result
    assert "Success rate below threshold" in result


def test_build_report_structure(healthy_metric, sample_alert):
    ts = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    report = build_report(healthy_metric, [sample_alert], timestamp=ts)
    assert report["timestamp"] == "2024-01-15T12:00:00+00:00"
    assert report["healthy"] is True
    assert report["alert_count"] == 1
    assert report["alerts"][0]["rule"] == "low_success_rate"
    assert report["alerts"][0]["severity"] == "CRITICAL"


def test_build_report_no_alerts(healthy_metric):
    report = build_report(healthy_metric, [])
    assert report["alert_count"] == 0
    assert report["alerts"] == []


def test_report_to_json(healthy_metric):
    report = build_report(healthy_metric, [])
    output = report_to_json(report)
    parsed = json.loads(output)
    assert parsed["alert_count"] == 0
    assert "metric" in parsed
