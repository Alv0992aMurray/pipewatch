"""Tests for pipewatch.drift_reporter."""
from __future__ import annotations

import json
from typing import Optional

import pytest

from pipewatch.drift import DriftResult
from pipewatch.drift_reporter import (
    format_drift_result,
    format_drift_report,
    drift_report_to_json,
)


def _result(drifted: bool = False, relative_change: Optional[float] = -0.05) -> DriftResult:
    return DriftResult(
        pipeline="orders",
        metric="success_rate",
        reference_mean=0.95,
        recent_mean=0.90,
        delta=-0.05,
        relative_change=relative_change,
        drifted=drifted,
        threshold=0.10,
    )


def test_format_result_stable_shows_stable():
    out = format_drift_result(_result(drifted=False))
    assert "stable" in out
    assert "DRIFT DETECTED" not in out


def test_format_result_drifted_shows_drift_detected():
    out = format_drift_result(_result(drifted=True))
    assert "DRIFT DETECTED" in out


def test_format_result_contains_pipeline_name():
    out = format_drift_result(_result())
    assert "orders" in out


def test_format_result_contains_metric_name():
    out = format_drift_result(_result())
    assert "success_rate" in out


def test_format_result_contains_relative_change():
    out = format_drift_result(_result(relative_change=-0.2631))
    assert "%" in out


def test_format_result_none_relative_change_shows_na():
    r = _result(relative_change=None)
    out = format_drift_result(r)
    assert "n/a" in out


def test_format_report_empty_list():
    out = format_drift_report([])
    assert "No drift results" in out


def test_format_report_multiple_results():
    results = [_result(drifted=False), _result(drifted=True)]
    out = format_drift_report(results)
    assert "stable" in out
    assert "DRIFT DETECTED" in out


def test_drift_report_to_json_is_valid():
    results = [_result(drifted=True)]
    raw = drift_report_to_json(results)
    parsed = json.loads(raw)
    assert isinstance(parsed, list)
    assert len(parsed) == 1
    assert parsed[0]["drifted"] is True
    assert parsed[0]["pipeline"] == "orders"


def test_drift_report_to_json_empty():
    raw = drift_report_to_json([])
    assert json.loads(raw) == []
