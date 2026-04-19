"""Tests for pipewatch.heatmap."""
from datetime import datetime, timezone
import pytest
from pipewatch.history import PipelineHistory, MetricSnapshot
from pipewatch.heatmap import build_heatmap, HeatmapCell, HeatmapResult


def _ts(hour: int) -> datetime:
    return datetime(2024, 1, 15, hour, 0, 0, tzinfo=timezone.utc)


def _snap(hour: int, success_rate: float, is_healthy: bool) -> MetricSnapshot:
    return MetricSnapshot(
        pipeline="pipe",
        timestamp=_ts(hour),
        success_rate=success_rate,
        throughput=100.0,
        error_rate=1.0 - success_rate,
        is_healthy=is_healthy,
    )


def _history(snaps):
    h = PipelineHistory(pipeline="pipe")
    for s in snaps:
        h.add(s)
    return h


def test_empty_history_returns_none():
    h = PipelineHistory(pipeline="pipe")
    assert build_heatmap(h) is None


def test_single_snapshot_creates_one_cell():
    h = _history([_snap(9, 0.99, True)])
    result = build_heatmap(h)
    assert result is not None
    assert len(result.cells) == 1
    assert result.cells[0].hour == 9
    assert result.cells[0].sample_count == 1


def test_multiple_hours_creates_multiple_cells():
    snaps = [_snap(8, 0.95, True), _snap(9, 0.80, False), _snap(10, 0.99, True)]
    result = build_heatmap(_history(snaps))
    assert len(result.cells) == 3
    hours = [c.hour for c in result.cells]
    assert hours == sorted(hours)


def test_avg_success_rate_aggregated_per_hour():
    snaps = [_snap(8, 0.80, False), _snap(8, 0.60, False)]
    result = build_heatmap(_history(snaps))
    assert len(result.cells) == 1
    assert abs(result.cells[0].avg_success_rate - 0.70) < 1e-9


def test_healthy_unhealthy_counts():
    snaps = [_snap(10, 0.99, True), _snap(10, 0.50, False), _snap(10, 0.97, True)]
    result = build_heatmap(_history(snaps))
    cell = result.cells[0]
    assert cell.healthy_count == 2
    assert cell.unhealthy_count == 1


def test_worst_and_best_hour():
    snaps = [
        _snap(6, 0.40, False),
        _snap(12, 0.99, True),
        _snap(18, 0.70, True),
    ]
    result = build_heatmap(_history(snaps))
    assert result.worst_hour() == 6
    assert result.best_hour() == 12


def test_to_dict_contains_expected_keys():
    h = _history([_snap(9, 0.95, True)])
    result = build_heatmap(h)
    d = result.to_dict()
    assert "pipeline" in d
    assert "cells" in d
    assert "worst_hour" in d
    assert "best_hour" in d


def test_cell_to_dict_structure():
    cell = HeatmapCell(hour=8, sample_count=3, avg_success_rate=0.85,
                       healthy_count=2, unhealthy_count=1)
    d = cell.to_dict()
    assert d["hour"] == 8
    assert d["sample_count"] == 3
    assert d["healthy_count"] == 2
    assert d["unhealthy_count"] == 1
