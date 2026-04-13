"""Tests for pipewatch.dedup_reporter."""
from __future__ import annotations

import json

import pytest

from pipewatch.alerts import Alert, AlertSeverity
from pipewatch.dedup import DedupResult
from pipewatch.dedup_reporter import (
    format_dedup_result,
    format_dedup_entries,
    dedup_report_to_json,
)
from pipewatch.dedup import AlertDeduplicator


def _alert(pipeline: str = "orders", severity: AlertSeverity = AlertSeverity.WARNING) -> Alert:
    return Alert(
        pipeline=pipeline,
        rule_name="low_success",
        severity=severity,
        message="success rate below threshold",
        metric_value=0.5,
        threshold=0.9,
    )


@pytest.fixture()
def empty_result() -> DedupResult:
    return DedupResult()


@pytest.fixture()
def mixed_result() -> DedupResult:
    r = DedupResult()
    r.kept.append(_alert(pipeline="orders"))
    r.suppressed.append(_alert(pipeline="payments", severity=AlertSeverity.CRITICAL))
    return r


def test_format_empty_result(empty_result):
    out = format_dedup_result(empty_result)
    assert "no alerts" in out


def test_format_result_shows_kept(mixed_result):
    out = format_dedup_result(mixed_result)
    assert "Forwarded" in out
    assert "orders" in out


def test_format_result_shows_suppressed(mixed_result):
    out = format_dedup_result(mixed_result)
    assert "Suppressed" in out
    assert "payments" in out


def test_format_entries_empty():
    out = format_dedup_entries([])
    assert "no dedup history" in out


def test_format_entries_shows_pipeline():
    dedup = AlertDeduplicator(cooldown_seconds=60)
    dedup._now = lambda: 1_000_000.0
    dedup.process([_alert(pipeline="orders")])
    out = format_dedup_entries(dedup.entries())
    assert "orders" in out
    assert "fired=1" in out


def test_json_output_structure(mixed_result):
    raw = dedup_report_to_json(mixed_result)
    data = json.loads(raw)
    assert "kept" in data
    assert "suppressed" in data
    assert data["total_suppressed"] == 1
    assert len(data["kept"]) == 1
