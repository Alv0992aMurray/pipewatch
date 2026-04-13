"""Tests for pipewatch.baseline."""

from __future__ import annotations

import json
import pytest
from pathlib import Path

from pipewatch.metrics import PipelineMetric
from pipewatch.baseline import (
    BaselineEntry,
    capture_baseline,
    compare_to_baseline,
    save_baseline,
    load_baseline,
)


@pytest.fixture
def healthy_metric() -> PipelineMetric:
    return PipelineMetric(
        pipeline_id="pipe-a",
        total_rows=1000,
        failed_rows=50,
        duration_seconds=100.0,
    )


@pytest.fixture
def degraded_metric() -> PipelineMetric:
    return PipelineMetric(
        pipeline_id="pipe-a",
        total_rows=1000,
        failed_rows=300,
        duration_seconds=200.0,
    )


def test_capture_baseline(healthy_metric):
    entry = capture_baseline(healthy_metric)
    assert entry.pipeline_id == "pipe-a"
    assert entry.success_rate == pytest.approx(0.95)
    assert entry.throughput == pytest.approx(10.0)


def test_compare_no_regression(healthy_metric):
    baseline = capture_baseline(healthy_metric)
    delta = compare_to_baseline(healthy_metric, baseline)
    assert delta.success_rate_delta == pytest.approx(0.0)
    assert delta.throughput_delta == pytest.approx(0.0)
    assert delta.regressed is False


def test_compare_detects_regression(healthy_metric, degraded_metric):
    baseline = capture_baseline(healthy_metric)
    delta = compare_to_baseline(degraded_metric, baseline, tolerance=0.05)
    assert delta.success_rate_delta < 0
    assert delta.regressed is True


def test_compare_within_tolerance(healthy_metric):
    slightly_worse = PipelineMetric(
        pipeline_id="pipe-a",
        total_rows=1000,
        failed_rows=60,  # 94% success vs 95% baseline — within 5%
        duration_seconds=102.0,
    )
    baseline = capture_baseline(healthy_metric)
    delta = compare_to_baseline(slightly_worse, baseline, tolerance=0.05)
    assert delta.regressed is False


def test_save_and_load_baseline(tmp_path, healthy_metric):
    path = tmp_path / "baseline.json"
    entry = capture_baseline(healthy_metric)
    save_baseline(entry, path=path)
    loaded = load_baseline("pipe-a", path=path)
    assert loaded is not None
    assert loaded.pipeline_id == "pipe-a"
    assert loaded.success_rate == pytest.approx(entry.success_rate)


def test_load_baseline_missing_file(tmp_path):
    result = load_baseline("pipe-x", path=tmp_path / "nonexistent.json")
    assert result is None


def test_load_baseline_missing_pipeline(tmp_path, healthy_metric):
    path = tmp_path / "baseline.json"
    entry = capture_baseline(healthy_metric)
    save_baseline(entry, path=path)
    result = load_baseline("pipe-z", path=path)
    assert result is None


def test_save_baseline_merges_entries(tmp_path):
    path = tmp_path / "baseline.json"
    e1 = BaselineEntry(pipeline_id="a", success_rate=0.9, throughput=5.0)
    e2 = BaselineEntry(pipeline_id="b", success_rate=0.8, throughput=3.0)
    save_baseline(e1, path=path)
    save_baseline(e2, path=path)
    data = json.loads(path.read_text())
    assert "a" in data and "b" in data
