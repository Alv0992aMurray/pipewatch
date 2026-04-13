"""Tests for pipewatch.anomaly_reporter module."""

from __future__ import annotations

import json

import pytest

from pipewatch.anomaly import AnomalyResult
from pipewatch.anomaly_reporter import (
    anomaly_report_to_json,
    format_anomaly_report,
    format_anomaly_result,
)


@pytest.fixture
def ok_result() -> AnomalyResult:
    return AnomalyResult(
        pipeline="pipe_a",
        metric="success_rate",
        current_value=0.95,
        mean=0.94,
        std_dev=0.01,
        z_score=1.0,
        is_anomaly=False,
        message="success_rate value 0.9500 is within normal range (z=1.00)",
    )


@pytest.fixture
def anomaly_result() -> AnomalyResult:
    return AnomalyResult(
        pipeline="pipe_b",
        metric="success_rate",
        current_value=0.10,
        mean=0.95,
        std_dev=0.01,
        z_score=-85.0,
        is_anomaly=True,
        message="success_rate value 0.1000 is below normal range (z=-85.00, threshold=2.0)",
    )


def test_format_anomaly_result_ok(ok_result):
    line = format_anomaly_result(ok_result)
    assert "[OK]" in line
    assert "pipe_a" in line
    assert "success_rate" in line


def test_format_anomaly_result_anomaly(anomaly_result):
    line = format_anomaly_result(anomaly_result)
    assert "[ANOMALY]" in line
    assert "pipe_b" in line


def test_format_anomaly_report_empty():
    report = format_anomaly_report([])
    assert "No anomaly results" in report


def test_format_anomaly_report_summary(ok_result, anomaly_result):
    report = format_anomaly_report([ok_result, anomaly_result])
    assert "Anomaly Detection Report" in report
    assert "1 anomaly detected" in report
    assert "2 check(s)" in report


def test_anomaly_report_to_json(ok_result, anomaly_result):
    output = anomaly_report_to_json([ok_result, anomaly_result])
    data = json.loads(output)
    assert len(data) == 2
    assert data[0]["pipeline"] == "pipe_a"
    assert data[1]["is_anomaly"] is True
