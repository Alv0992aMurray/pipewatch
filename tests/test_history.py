"""Tests for pipewatch.history module."""

from __future__ import annotations

import json
import os

import pytest

from pipewatch.history import (
    MetricSnapshot,
    PipelineHistory,
    load_history,
    record_metric,
    save_history,
)
from pipewatch.metrics import PipelineMetric


@pytest.fixture
def healthy_metric() -> PipelineMetric:
    return PipelineMetric(
        pipeline_name="orders",
        rows_processed=1000,
        rows_failed=10,
        error_count=2,
        duration_seconds=30.0,
    )


@pytest.fixture
def failing_metric() -> PipelineMetric:
    return PipelineMetric(
        pipeline_name="orders",
        rows_processed=100,
        rows_failed=60,
        error_count=15,
        duration_seconds=10.0,
    )


def test_snapshot_from_healthy_metric(healthy_metric):
    snap = MetricSnapshot.from_metric(healthy_metric)
    assert snap.pipeline == "orders"
    assert snap.success_rate == pytest.approx(0.99, abs=0.01)
    assert snap.is_healthy is True
    assert snap.error_count == 2
    assert snap.timestamp  # non-empty string


def test_snapshot_from_failing_metric(failing_metric):
    snap = MetricSnapshot.from_metric(failing_metric)
    assert snap.success_rate < 0.5
    assert snap.is_healthy is False


def test_pipeline_history_add_and_last_n(healthy_metric):
    history = PipelineHistory(pipeline="orders")
    for _ in range(5):
        history.add(MetricSnapshot.from_metric(healthy_metric))
    assert len(history.snapshots) == 5
    assert len(history.last_n(3)) == 3


def test_average_success_rate(healthy_metric, failing_metric):
    history = PipelineHistory(pipeline="orders")
    history.add(MetricSnapshot.from_metric(healthy_metric))  # ~0.99
    history.add(MetricSnapshot.from_metric(failing_metric))  # ~0.40
    avg = history.average_success_rate()
    assert avg is not None
    assert 0.4 < avg < 0.99


def test_average_success_rate_empty():
    history = PipelineHistory(pipeline="empty")
    assert history.average_success_rate() is None


def test_save_and_load_history(tmp_path, healthy_metric):
    path = str(tmp_path / "history.json")
    snap = MetricSnapshot.from_metric(healthy_metric)
    ph = PipelineHistory(pipeline="orders", snapshots=[snap])
    save_history({"orders": ph}, path=path)

    loaded = load_history(path)
    assert "orders" in loaded
    assert len(loaded["orders"].snapshots) == 1
    assert loaded["orders"].snapshots[0].pipeline == "orders"


def test_load_history_missing_file(tmp_path):
    result = load_history(str(tmp_path / "nonexistent.json"))
    assert result == {}


def test_record_metric_creates_and_appends(tmp_path, healthy_metric):
    path = str(tmp_path / "history.json")
    snap1 = record_metric(healthy_metric, path=path)
    snap2 = record_metric(healthy_metric, path=path)

    assert snap1.pipeline == "orders"
    loaded = load_history(path)
    assert len(loaded["orders"].snapshots) == 2


def test_record_metric_multiple_pipelines(tmp_path, healthy_metric):
    """Recording metrics for different pipelines stores them independently."""
    invoices_metric = PipelineMetric(
        pipeline_name="invoices",
        rows_processed=500,
        rows_failed=5,
        error_count=1,
        duration_seconds=15.0,
    )
    path = str(tmp_path / "history.json")
    record_metric(healthy_metric, path=path)
    record_metric(invoices_metric, path=path)

    loaded = load_history(path)
    assert "orders" in loaded
    assert "invoices" in loaded
    assert len(loaded["orders"].snapshots) == 1
    assert len(loaded["invoices"].snapshots) == 1
