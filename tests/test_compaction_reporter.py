"""Tests for pipewatch.compaction_reporter."""
from __future__ import annotations

import json
from datetime import datetime, timedelta

from pipewatch.compaction import CompactionBucket, CompactionResult
from pipewatch.compaction_reporter import (
    format_bucket,
    format_compaction_report,
    compaction_report_to_json,
)


def _dt(h: int, m: int) -> datetime:
    return datetime(2024, 1, 1, h, m, 0)


@pytest.fixture
def sample_bucket():
    return CompactionBucket(
        start=_dt(10, 0),
        end=_dt(10, 15),
        snapshot_count=4,
        avg_success_rate=0.92,
        avg_throughput=55.5,
        any_unhealthy=False,
    )


@pytest.fixture
def unhealthy_bucket():
    return CompactionBucket(
        start=_dt(9, 45),
        end=_dt(10, 0),
        snapshot_count=2,
        avg_success_rate=0.4,
        avg_throughput=10.0,
        any_unhealthy=True,
    )


@pytest.fixture
def sample_result(sample_bucket):
    return CompactionResult(
        pipeline="orders",
        bucket_size_minutes=15,
        buckets=[sample_bucket],
        retained_snapshots=[],
    )


import pytest


def test_format_bucket_healthy_shows_checkmark(sample_bucket):
    line = format_bucket(sample_bucket)
    assert "✓" in line


def test_format_bucket_unhealthy_shows_warning(unhealthy_bucket):
    line = format_bucket(unhealthy_bucket)
    assert "⚠" in line


def test_format_bucket_contains_success_rate(sample_bucket):
    line = format_bucket(sample_bucket)
    assert "92.0%" in line


def test_format_report_contains_pipeline_name(sample_result):
    report = format_compaction_report([sample_result])
    assert "orders" in report


def test_format_report_empty_list():
    report = format_compaction_report([])
    assert "No compaction" in report


def test_format_report_no_buckets_shows_message():
    r = CompactionResult(pipeline="x", bucket_size_minutes=15, buckets=[], retained_snapshots=[])
    report = format_compaction_report([r])
    assert "No compacted buckets" in report


def test_json_output_is_valid(sample_result):
    out = compaction_report_to_json([sample_result])
    data = json.loads(out)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "orders"
