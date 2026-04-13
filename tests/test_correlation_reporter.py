"""Tests for pipewatch.correlation_reporter."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import List

import pytest

from pipewatch.correlation import CorrelationResult
from pipewatch.correlation_reporter import (
    format_correlation_result,
    format_correlation_report,
    correlation_report_to_json,
)


@pytest.fixture()
def strong_positive() -> CorrelationResult:
    return CorrelationResult(
        pipeline_a="orders",
        pipeline_b="payments",
        r=0.95,
        strength="strong",
        direction="positive",
        interpretation="strong positive correlation",
    )


@pytest.fixture()
def weak_negative() -> CorrelationResult:
    return CorrelationResult(
        pipeline_a="orders",
        pipeline_b="refunds",
        r=-0.21,
        strength="weak",
        direction="negative",
        interpretation="weak negative correlation",
    )


@pytest.fixture()
def no_data() -> CorrelationResult:
    return CorrelationResult(
        pipeline_a="alpha",
        pipeline_b="beta",
        r=None,
        strength="none",
        direction="none",
        interpretation="insufficient data",
    )


def test_format_result_strong_positive(strong_positive):
    line = format_correlation_result(strong_positive)
    assert "orders" in line
    assert "payments" in line
    assert "Strong" in line
    assert "(+)" in line
    assert "r=+0.950" in line


def test_format_result_weak_negative(weak_negative):
    line = format_correlation_result(weak_negative)
    assert "Weak" in line
    assert "(-)" in line
    assert "r=-0.210" in line


def test_format_result_no_data(no_data):
    line = format_correlation_result(no_data)
    assert "insufficient data" in line
    assert "alpha" in line
    assert "beta" in line


def test_format_report_empty():
    report = format_correlation_report([])
    assert "no pairs" in report
    assert "Correlation Report" in report


def test_format_report_multiple(strong_positive, weak_negative):
    report = format_correlation_report([strong_positive, weak_negative])
    assert "Correlation Report" in report
    assert "orders" in report
    assert "refunds" in report


def test_correlation_report_to_json(strong_positive, weak_negative):
    output = correlation_report_to_json([strong_positive, weak_negative])
    data = json.loads(output)
    assert isinstance(data, list)
    assert len(data) == 2
    assert data[0]["pipeline_a"] == "orders"
    assert data[0]["r"] == pytest.approx(0.95)


def test_correlation_report_to_json_empty():
    output = correlation_report_to_json([])
    assert json.loads(output) == []
