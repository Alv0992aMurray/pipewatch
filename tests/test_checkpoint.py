"""Tests for pipewatch.checkpoint and pipewatch.checkpoint_reporter."""

from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta

import pytest

from pipewatch.checkpoint import CheckpointEntry, CheckpointStore
from pipewatch.checkpoint_reporter import (
    format_checkpoint_report,
    checkpoint_report_to_json,
    format_entry,
)


_BASE = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def store() -> CheckpointStore:
    return CheckpointStore()


def test_record_creates_entry(store):
    entry = store.record("pipeline_a", "ok", at=_BASE)
    assert entry.pipeline == "pipeline_a"
    assert entry.last_status == "ok"
    assert entry.run_count == 1
    assert entry.last_run == _BASE


def test_record_increments_run_count(store):
    store.record("pipeline_a", "ok", at=_BASE)
    entry = store.record("pipeline_a", "error", at=_BASE + timedelta(minutes=5))
    assert entry.run_count == 2
    assert entry.last_status == "error"


def test_get_returns_none_for_unknown_pipeline(store):
    assert store.get("missing") is None


def test_get_returns_recorded_entry(store):
    store.record("pipe", "ok", at=_BASE)
    entry = store.get("pipe")
    assert entry is not None
    assert entry.pipeline == "pipe"


def test_seconds_since_last_run(store):
    store.record("pipe", "ok", at=_BASE)
    now = _BASE + timedelta(seconds=90)
    age = store.seconds_since_last_run("pipe", now=now)
    assert age == pytest.approx(90.0)


def test_seconds_since_last_run_unknown_returns_none(store):
    assert store.seconds_since_last_run("ghost") is None


def test_clear_removes_entry(store):
    store.record("pipe", "ok", at=_BASE)
    store.clear("pipe")
    assert store.get("pipe") is None


def test_to_dict_and_from_dict_roundtrip():
    entry = CheckpointEntry(pipeline="p", last_run=_BASE, run_count=3, last_status="ok")
    restored = CheckpointEntry.from_dict(entry.to_dict())
    assert restored.pipeline == entry.pipeline
    assert restored.run_count == entry.run_count
    assert restored.last_status == entry.last_status
    assert restored.last_run == entry.last_run


def test_format_checkpoint_report_empty(store):
    report = format_checkpoint_report(store)
    assert "no pipelines" in report


def test_format_checkpoint_report_shows_pipeline(store):
    store.record("alpha", "ok", at=_BASE)
    now = _BASE + timedelta(seconds=30)
    report = format_checkpoint_report(store, now=now)
    assert "alpha" in report
    assert "30s ago" in report
    assert "✓" in report


def test_format_entry_error_status(store):
    store.record("beta", "error", at=_BASE)
    now = _BASE + timedelta(minutes=2)
    entry = store.get("beta")
    line = format_entry(entry, now=now)
    assert "✗" in line
    assert "2m ago" in line


def test_checkpoint_report_to_json(store):
    store.record("pipe", "ok", at=_BASE)
    payload = json.loads(checkpoint_report_to_json(store))
    assert "checkpoints" in payload
    assert payload["checkpoints"][0]["pipeline"] == "pipe"
