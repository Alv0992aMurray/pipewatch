"""Tests for cascade failure detection."""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import List

import pytest

from pipewatch.history import PipelineHistory, MetricSnapshot
from pipewatch.cascade import detect_cascade, CascadeResult
from pipewatch.cascade_reporter import format_cascade_report, cascade_report_to_json


def _ts(offset_seconds: int = 0) -> datetime:
    base = datetime(2024, 1, 1, 12, 0, 0)
    return base + timedelta(seconds=offset_seconds)


def _snap(pipeline: str, healthy: bool, offset: int) -> MetricSnapshot:
    return MetricSnapshot(
        pipeline_name=pipeline,
        timestamp=_ts(offset),
        success_rate=1.0 if healthy else 0.2,
        throughput=100.0,
        error_count=0 if healthy else 5,
        healthy=healthy,
    )


def _history(pipeline: str, snaps: List[MetricSnapshot]) -> PipelineHistory:
    h = PipelineHistory(pipeline_name=pipeline)
    for s in snaps:
        h.add(s)
    return h


def test_no_edges_returns_empty_result():
    histories = {}
    result = detect_cascade(histories, [])
    assert result.total_links == 0


def test_missing_pipeline_skipped():
    h = _history("A", [_snap("A", False, 0)])
    result = detect_cascade({"A": h}, [("A", "B")])
    assert result.total_links == 0


def test_no_failures_produces_no_links():
    ha = _history("A", [_snap("A", True, 0), _snap("A", True, 60)])
    hb = _history("B", [_snap("B", True, 30), _snap("B", True, 90)])
    result = detect_cascade({"A": ha, "B": hb}, [("A", "B")])
    assert result.total_links == 0


def test_downstream_failure_after_upstream_within_window():
    ha = _history("A", [_snap("A", False, 0)])
    hb = _history("B", [_snap("B", False, 120)])
    result = detect_cascade({"A": ha, "B": hb}, [("A", "B")], window_seconds=300)
    assert result.total_links == 1
    assert result.links[0].upstream == "A"
    assert result.links[0].downstream == "B"
    assert result.links[0].overlap_count == 1


def test_downstream_failure_before_upstream_not_counted():
    ha = _history("A", [_snap("A", False, 200)])
    hb = _history("B", [_snap("B", False, 0)])
    result = detect_cascade({"A": ha, "B": hb}, [("A", "B")], window_seconds=300)
    assert result.total_links == 0


def test_downstream_failure_outside_window_not_counted():
    ha = _history("A", [_snap("A", False, 0)])
    hb = _history("B", [_snap("B", False, 600)])
    result = detect_cascade({"A": ha, "B": hb}, [("A", "B")], window_seconds=300)
    assert result.total_links == 0


def test_affected_pipelines_lists_all_involved():
    ha = _history("A", [_snap("A", False, 0)])
    hb = _history("B", [_snap("B", False, 60)])
    result = detect_cascade({"A": ha, "B": hb}, [("A", "B")], window_seconds=300)
    affected = result.affected_pipelines()
    assert "A" in affected
    assert "B" in affected


def test_format_report_no_links():
    result = CascadeResult(links=[])
    report = format_cascade_report(result)
    assert "No cascade" in report


def test_format_report_with_links():
    ha = _history("pipe_a", [_snap("pipe_a", False, 0)])
    hb = _history("pipe_b", [_snap("pipe_b", False, 100)])
    result = detect_cascade({"pipe_a": ha, "pipe_b": hb}, [("pipe_a", "pipe_b")])
    report = format_cascade_report(result)
    assert "pipe_a" in report
    assert "pipe_b" in report


def test_cascade_report_to_json():
    result = CascadeResult(links=[])
    out = cascade_report_to_json(result)
    import json
    data = json.loads(out)
    assert "links" in data
