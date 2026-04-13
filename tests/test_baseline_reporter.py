"""Tests for pipewatch.baseline_reporter."""

from __future__ import annotations

import json
import pytest

from pipewatch.baseline import BaselineDelta
from pipewatch.baseline_reporter import (
    format_delta,
    format_baseline_report,
    baseline_report_to_json,
)


@pytest.fixture
def ok_delta() -> BaselineDelta:
    return BaselineDelta(
        pipeline_id="pipe-a",
        success_rate_delta=0.02,
        throughput_delta=1.5,
        regressed=False,
    )


@pytest.fixture
def regressed_delta() -> BaselineDelta:
    return BaselineDelta(
        pipeline_id="pipe-b",
        success_rate_delta=-0.12,
        throughput_delta=-3.0,
        regressed=True,
    )


def test_format_delta_ok(ok_delta):
    line = format_delta(ok_delta)
    assert "[OK]" in line
    assert "pipe-a" in line
    assert "+" in line  # positive deltas show sign


def test_format_delta_regressed(regressed_delta):
    line = format_delta(regressed_delta)
    assert "[REGRESSED]" in line
    assert "pipe-b" in line


def test_format_baseline_report_empty():
    report = format_baseline_report([])
    assert "No baseline" in report


def test_format_baseline_report_all_ok(ok_delta):
    report = format_baseline_report([ok_delta])
    assert "All pipelines within baseline tolerance" in report
    assert "[OK]" in report


def test_format_baseline_report_with_regression(ok_delta, regressed_delta):
    report = format_baseline_report([ok_delta, regressed_delta])
    assert "WARNING" in report
    assert "pipe-b" in report


def test_baseline_report_to_json(ok_delta, regressed_delta):
    output = baseline_report_to_json([ok_delta, regressed_delta])
    data = json.loads(output)
    assert "baseline_comparison" in data
    assert len(data["baseline_comparison"]) == 2
    ids = [e["pipeline_id"] for e in data["baseline_comparison"]]
    assert "pipe-a" in ids and "pipe-b" in ids


def test_baseline_report_to_json_regression_flag(regressed_delta):
    output = baseline_report_to_json([regressed_delta])
    data = json.loads(output)
    entry = data["baseline_comparison"][0]
    assert entry["regressed"] is True
