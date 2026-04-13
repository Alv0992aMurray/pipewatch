"""Tests for pipewatch.export — CSV and JSON Lines export helpers."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from pipewatch.export import export_history, snapshots_to_csv, snapshots_to_jsonl
from pipewatch.history import MetricSnapshot, PipelineHistory


_TS = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


def _snap(pipeline: str = "orders", success_rate: float = 0.99) -> MetricSnapshot:
    total = 1000
    passed = int(total * success_rate)
    return MetricSnapshot(
        pipeline=pipeline,
        timestamp=_TS,
        total_rows=total,
        passed_rows=passed,
        failed_rows=total - passed,
        success_rate=success_rate,
        throughput_per_second=50.0,
        is_healthy=success_rate >= 0.95,
    )


# ---------------------------------------------------------------------------
# snapshots_to_csv
# ---------------------------------------------------------------------------

def test_csv_header_present():
    output = snapshots_to_csv([])
    assert "pipeline" in output
    assert "success_rate" in output


def test_csv_empty_snapshots_only_header():
    output = snapshots_to_csv([])
    lines = [l for l in output.splitlines() if l]
    assert len(lines) == 1  # header only


def test_csv_single_snapshot_one_data_row():
    output = snapshots_to_csv([_snap()])
    lines = [l for l in output.splitlines() if l]
    assert len(lines) == 2  # header + 1 row
    assert "orders" in lines[1]


def test_csv_multiple_snapshots():
    snaps = [_snap("orders"), _snap("users", 0.80)]
    output = snapshots_to_csv(snaps)
    lines = [l for l in output.splitlines() if l]
    assert len(lines) == 3


def test_csv_is_healthy_field():
    output = snapshots_to_csv([_snap(success_rate=0.80)])
    assert "False" in output


# ---------------------------------------------------------------------------
# snapshots_to_jsonl
# ---------------------------------------------------------------------------

def test_jsonl_empty_returns_empty_string():
    assert snapshots_to_jsonl([]) == ""


def test_jsonl_single_snapshot_valid_json():
    output = snapshots_to_jsonl([_snap()])
    lines = [l for l in output.splitlines() if l]
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["pipeline"] == "orders"
    assert record["success_rate"] == pytest.approx(0.99)


def test_jsonl_multiple_snapshots():
    snaps = [_snap("a"), _snap("b")]
    output = snapshots_to_jsonl(snaps)
    lines = [l for l in output.splitlines() if l]
    assert len(lines) == 2
    assert json.loads(lines[1])["pipeline"] == "b"


def test_jsonl_contains_timestamp_iso():
    output = snapshots_to_jsonl([_snap()])
    record = json.loads(output.splitlines()[0])
    assert "2024-06-01" in record["timestamp"]


# ---------------------------------------------------------------------------
# export_history
# ---------------------------------------------------------------------------

def _make_history() -> PipelineHistory:
    h = PipelineHistory(pipeline="orders")
    h.add(_snap("orders", 0.99))
    h.add(_snap("orders", 0.97))
    return h


def test_export_history_csv():
    h = _make_history()
    output = export_history(h, fmt="csv")
    lines = [l for l in output.splitlines() if l]
    assert len(lines) == 3  # header + 2 rows


def test_export_history_jsonl():
    h = _make_history()
    output = export_history(h, fmt="jsonl")
    lines = [l for l in output.splitlines() if l]
    assert len(lines) == 2


def test_export_history_invalid_format_raises():
    h = _make_history()
    with pytest.raises(ValueError, match="Unsupported export format"):
        export_history(h, fmt="xml")
