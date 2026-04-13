"""Tests for pipewatch.digest and pipewatch.digest_reporter."""

import pytest

from pipewatch.metrics import PipelineMetric
from pipewatch.history import PipelineHistory, from_metric
from pipewatch.alerts import AlertRule, AlertSeverity
from pipewatch.digest import build_digest, DigestEntry
from pipewatch.digest_reporter import format_digest, digest_to_json
import json


def _make_history(name: str, rates, rows=100, errors=0) -> PipelineHistory:
    h = PipelineHistory(pipeline_name=name)
    for rate_hint in rates:
        processed = int(rows * rate_hint)
        m = PipelineMetric(
            pipeline_name=name,
            rows_processed=processed,
            rows_failed=rows - processed + errors,
            duration_seconds=10.0,
        )
        h.add(from_metric(m))
    return h


@pytest.fixture
def healthy_history():
    return _make_history("pipe_a", [1.0, 1.0, 1.0, 1.0, 1.0])


@pytest.fixture
def failing_history():
    return _make_history("pipe_b", [0.5, 0.4, 0.3], errors=5)


@pytest.fixture
def low_success_rule():
    return AlertRule(
        metric="success_rate",
        threshold=0.8,
        operator="lt",
        severity=AlertSeverity.CRITICAL,
        message="Success rate below 80%",
    )


def test_build_digest_no_histories():
    digest = build_digest([], rules=[])
    assert digest.entries == []
    assert digest.total_alerts() == 0


def test_build_digest_healthy_pipeline(healthy_history, low_success_rule):
    digest = build_digest([healthy_history], rules=[low_success_rule])
    assert len(digest.entries) == 1
    entry = digest.entries[0]
    assert entry.pipeline_name == "pipe_a"
    assert entry.snapshot_count == 5
    assert entry.alerts == []
    assert not entry.has_alerts()


def test_build_digest_failing_pipeline_triggers_alert(failing_history, low_success_rule):
    digest = build_digest([failing_history], rules=[low_success_rule])
    entry = digest.entries[0]
    assert entry.has_alerts()
    assert len(entry.alerts) == 1
    assert entry.alerts[0].severity == AlertSeverity.CRITICAL


def test_build_digest_multiple_pipelines(healthy_history, failing_history, low_success_rule):
    digest = build_digest([healthy_history, failing_history], rules=[low_success_rule])
    assert len(digest.entries) == 2
    assert len(digest.pipelines_with_alerts()) == 1
    assert digest.total_alerts() == 1


def test_format_digest_contains_pipeline_name(healthy_history):
    digest = build_digest([healthy_history], rules=[])
    output = format_digest(digest)
    assert "pipe_a" in output
    assert "PipeWatch Digest" in output


def test_format_digest_empty():
    from pipewatch.digest import Digest
    output = format_digest(Digest(entries=[]))
    assert "No pipeline data" in output


def test_digest_to_json_structure(healthy_history, failing_history, low_success_rule):
    digest = build_digest([healthy_history, failing_history], rules=[low_success_rule])
    raw = digest_to_json(digest)
    data = json.loads(raw)
    assert "pipelines" in data
    assert "total_alerts" in data
    assert len(data["pipelines"]) == 2
    pipe_names = {p["pipeline_name"] for p in data["pipelines"]}
    assert "pipe_a" in pipe_names
    assert "pipe_b" in pipe_names
