"""Tests for pipewatch.reachability."""
import pytest
from pipewatch.reachability import analyse_reachability


@pytest.fixture()
def simple_graph():
    return {
        "ingest": ["transform", "validate"],
        "transform": ["load"],
        "validate": ["load"],
        "load": [],
    }


def test_source_is_not_in_reachable(simple_graph):
    result = analyse_reachability("ingest", simple_graph)
    assert "ingest" not in result.reachable


def test_all_downstream_reachable(simple_graph):
    result = analyse_reachability("ingest", simple_graph)
    assert set(result.reachable) == {"transform", "validate", "load"}


def test_depth_map_correct(simple_graph):
    result = analyse_reachability("ingest", simple_graph)
    assert result.depth_map["transform"] == 1
    assert result.depth_map["validate"] == 1
    assert result.depth_map["load"] == 2


def test_unreachable_when_isolated():
    graph = {"a": ["b"], "b": [], "c": []}
    result = analyse_reachability("a", graph, all_nodes=["a", "b", "c"])
    assert result.unreachable == ["c"]


def test_no_unreachable_when_fully_connected(simple_graph):
    result = analyse_reachability("ingest", simple_graph)
    assert result.unreachable == []


def test_leaf_node_has_no_reachable(simple_graph):
    result = analyse_reachability("load", simple_graph)
    assert result.reachable == []


def test_all_nodes_inferred_from_graph(simple_graph):
    result = analyse_reachability("ingest", simple_graph)
    assert result.total_reachable == 3


def test_explicit_all_nodes_respected():
    graph = {"a": ["b"]}
    result = analyse_reachability("a", graph, all_nodes=["a", "b", "orphan"])
    assert "orphan" in result.unreachable


def test_to_dict_keys():
    graph = {"a": ["b"], "b": []}
    result = analyse_reachability("a", graph)
    d = result.to_dict()
    assert set(d.keys()) == {"source", "reachable", "unreachable", "depth_map"}


def test_cyclic_graph_does_not_loop():
    graph = {"a": ["b"], "b": ["c"], "c": ["a"]}
    result = analyse_reachability("a", graph)
    assert set(result.reachable) == {"b", "c"}
