"""Tests for pipewatch.sampling and pipewatch.sampling_reporter."""
from __future__ import annotations

import json
from datetime import datetime, timedelta

import pytest

from pipewatch.history import MetricSnapshot
from pipewatch.sampling import SamplingConfig, SampleWindow
from pipewatch.sampling_reporter import (
    format_sample_window,
    format_sampling_report,
    sampling_report_to_json,
)


BASE_TIME = datetime(2024, 6, 1, 12, 0, 0)


def _snap(offset_seconds: int = 0, success_rate: float = 1.0, throughput: float = 50.0) -> MetricSnapshot:
    return MetricSnapshot(
        pipeline_name="pipe",
        timestamp=BASE_TIME + timedelta(seconds=offset_seconds),
        success_rate=success_rate,
        throughput=throughput,
        error_count=0,
        is_healthy=True,
    )


@pytest.fixture
def config() -> SamplingConfig:
    return SamplingConfig(pipeline_name="pipe", window_seconds=300, max_samples=5)


def test_empty_window_reports_no_samples(config):
    w = SampleWindow(config)
    assert w.is_empty()
    assert w.average_success_rate() is None
    assert w.average_throughput() is None


def test_add_single_snapshot(config):
    w = SampleWindow(config)
    w.add(_snap())
    assert not w.is_empty()
    assert len(w.samples()) == 1


def test_average_success_rate(config):
    w = SampleWindow(config)
    w.add(_snap(success_rate=0.8))
    w.add(_snap(success_rate=0.6))
    assert w.average_success_rate() == pytest.approx(0.7)


def test_average_throughput(config):
    w = SampleWindow(config)
    w.add(_snap(throughput=100.0))
    w.add(_snap(throughput=200.0))
    assert w.average_throughput() == pytest.approx(150.0)


def test_samples_outside_window_are_pruned(config):
    w = SampleWindow(config)
    old = _snap(offset_seconds=0)
    recent = _snap(offset_seconds=400)  # 400s later — old is outside 300s window
    w.add(old)
    w.add(recent)
    assert len(w.samples()) == 1
    assert w.samples()[0].timestamp == recent.timestamp


def test_max_samples_cap_is_enforced(config):
    w = SampleWindow(config)
    for i in range(10):
        w.add(_snap(offset_seconds=i))
    assert len(w.samples()) <= config.max_samples


def test_to_dict_keys(config):
    w = SampleWindow(config)
    w.add(_snap())
    d = w.to_dict()
    assert "pipeline" in d
    assert "window_seconds" in d
    assert "sample_count" in d
    assert "average_success_rate" in d
    assert "average_throughput" in d


def test_format_sample_window_empty(config):
    w = SampleWindow(config)
    result = format_sample_window(w)
    assert "No samples" in result


def test_format_sample_window_with_data(config):
    w = SampleWindow(config)
    w.add(_snap(success_rate=0.95, throughput=42.0))
    result = format_sample_window(w)
    assert "pipe" in result
    assert "95.0%" in result
    assert "42.0 rows/s" in result


def test_format_sampling_report_empty():
    result = format_sampling_report([])
    assert "No sample windows" in result


def test_format_sampling_report_multiple(config):
    w1 = SampleWindow(config)
    w1.add(_snap())
    w2 = SampleWindow(SamplingConfig(pipeline_name="other", window_seconds=60))
    result = format_sampling_report([w1, w2])
    assert "Sampling Report" in result
    assert "pipe" in result
    assert "other" in result


def test_sampling_report_to_json(config):
    w = SampleWindow(config)
    w.add(_snap())
    raw = sampling_report_to_json([w])
    data = json.loads(raw)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "pipe"
