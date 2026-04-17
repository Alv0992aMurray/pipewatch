"""Tests for pipewatch.reachability_reporter."""
import json
import pytest
from pipewatch.reachability import analyse_reachability
from pipewatch.reachability_reporter import (
    format_reachability_result,
    format_reachability_report,
    reachability_report_to_json,
)


@pytest.fixture()
def simple_result():
    graph = {"ingest": ["transform", "load"], "transform": ["load"], "load": []}
    return analyse_reachability("ingest", graph)


@pytest.fixture()
def isolated_result():
    graph = {"a": [], "b": []}
    return analyse_reachability("a", graph, all_nodes=["a", "b"])


def test_format_result_contains_source(simple_result):
    text = format_reachability_result(simple_result)
    assert "ingest" in text


def test_format_result_lists_reachable(simple_result):
    text = format_reachability_result(simple_result)
    assert "transform" in text
    assert "load" in text


def test_format_result_shows_depth(simple_result):
    text = format_reachability_result(simple_result)
    assert "depth" in text


def test_format_result_shows_none_when_no_unreachable(simple_result):
    text = format_reachability_result(simple_result)
    assert "none" in text


def test_format_result_shows_unreachable(isolated_result):
    text = format_reachability_result(isolated_result)
    assert "b" in text


def test_format_report_empty():
    text = format_reachability_report([])
    assert "No reachability" in text


def test_format_report_multiple(simple_result, isolated_result):
    text = format_reachability_report([simple_result, isolated_result])
    assert "ingest" in text
    assert "'a'" in text


def test_json_output_is_valid(simple_result):
    raw = reachability_report_to_json([simple_result])
    data = json.loads(raw)
    assert isinstance(data, list)
    assert data[0]["source"] == "ingest"


def test_json_empty_list():
    raw = reachability_report_to_json([])
    assert json.loads(raw) == []
