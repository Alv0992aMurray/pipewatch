"""Tests for pipewatch.topology_reporter."""
import json
import pytest
from pipewatch.topology import build_topology
from pipewatch.topology_reporter import (
    format_topology_result,
    format_topology_report,
    topology_report_to_json,
)


def _result():
    return build_topology([("ingest", "transform"), ("transform", "load"), ("ingest", "audit")])


def test_format_result_none_shows_no_edges():
    out = format_topology_result(None)
    assert "no edges" in out


def test_format_result_shows_pipeline_count():
    out = format_topology_result(_result())
    assert "4" in out  # ingest, transform, load, audit


def test_format_result_shows_critical_path():
    out = format_topology_result(_result())
    assert "→" in out


def test_format_result_shows_bottleneck():
    out = format_topology_result(_result())
    assert "ingest" in out


def test_format_report_empty_list():
    out = format_topology_report([])
    assert "no topology" in out


def test_format_report_multiple_results():
    r = _result()
    out = format_topology_report([r, r])
    assert out.count("[topology]") == 2


def test_to_json_none():
    out = topology_report_to_json(None)
    data = json.loads(out)
    assert data["topology"] is None


def test_to_json_result_has_keys():
    out = topology_report_to_json(_result())
    data = json.loads(out)
    assert "critical_path" in data["topology"]
    assert "bottlenecks" in data["topology"]
