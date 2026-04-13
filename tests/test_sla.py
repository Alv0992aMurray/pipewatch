"""Tests for pipewatch.sla and pipewatch.sla_reporter."""
from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from pipewatch.history import PipelineHistory, MetricSnapshot
from pipewatch.sla import SLAConfig, evaluate_sla
from pipewatch.sla_reporter import format_sla_result, format_sla_report, sla_report_to_json


def _snap(success_rate: float, error_rate: float) -> MetricSnapshot:
    return MetricSnapshot(
        pipeline="orders",
        timestamp=datetime.now(timezone.utc),
        success_rate=success_rate,
        error_rate=error_rate,
        throughput=100.0,
    )


def _history(snaps):
    h = PipelineHistory(pipeline="orders")
    for s in snaps:
        h.add(s)
    return h


@pytest.fixture
def good_config():
    return SLAConfig(pipeline="orders", min_success_rate=0.95, max_error_rate=0.05, window=5)


def test_evaluate_sla_empty_history_returns_none(good_config):
    h = PipelineHistory(pipeline="orders")
    assert evaluate_sla(h, good_config) is None


def test_evaluate_sla_passes_when_within_thresholds(good_config):
    snaps = [_snap(0.98, 0.02) for _ in range(5)]
    result = evaluate_sla(_history(snaps), good_config)
    assert result is not None
    assert result.passed is True
    assert result.violations == []


def test_evaluate_sla_fails_low_success_rate(good_config):
    snaps = [_snap(0.80, 0.02) for _ in range(5)]
    result = evaluate_sla(_history(snaps), good_config)
    assert result is not None
    assert result.passed is False
    metrics = [v.metric for v in result.violations]
    assert "success_rate" in metrics


def test_evaluate_sla_fails_high_error_rate(good_config):
    snaps = [_snap(0.97, 0.10) for _ in range(5)]
    result = evaluate_sla(_history(snaps), good_config)
    assert result is not None
    assert result.passed is False
    metrics = [v.metric for v in result.violations]
    assert "error_rate" in metrics


def test_evaluate_sla_respects_window():
    config = SLAConfig(pipeline="orders", min_success_rate=0.95, max_error_rate=0.05, window=3)
    # first 7 bad, last 3 good — only last 3 should be evaluated
    snaps = [_snap(0.50, 0.50) for _ in range(7)] + [_snap(0.99, 0.01) for _ in range(3)]
    result = evaluate_sla(_history(snaps), config)
    assert result is not None
    assert result.passed is True


def test_violation_to_dict(good_config):
    snaps = [_snap(0.80, 0.02) for _ in range(5)]
    result = evaluate_sla(_history(snaps), good_config)
    d = result.violations[0].to_dict()
    assert d["metric"] == "success_rate"
    assert "threshold" in d
    assert "actual" in d
    assert "message" in d


def test_format_sla_result_passed(good_config):
    snaps = [_snap(0.98, 0.02) for _ in range(5)]
    result = evaluate_sla(_history(snaps), good_config)
    text = format_sla_result(result)
    assert "PASSED" in text
    assert "orders" in text


def test_format_sla_result_failed(good_config):
    snaps = [_snap(0.80, 0.02) for _ in range(5)]
    result = evaluate_sla(_history(snaps), good_config)
    text = format_sla_result(result)
    assert "FAILED" in text
    assert "success_rate" in text


def test_format_sla_report_empty():
    assert "No SLA results" in format_sla_report([])


def test_sla_report_to_json(good_config):
    snaps = [_snap(0.98, 0.02) for _ in range(5)]
    result = evaluate_sla(_history(snaps), good_config)
    payload = json.loads(sla_report_to_json([result]))
    assert isinstance(payload, list)
    assert payload[0]["pipeline"] == "orders"
    assert payload[0]["passed"] is True
