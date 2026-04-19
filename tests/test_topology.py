"""Tests for pipewatch.topology."""
import pytest
from pipewatch.topology import build_topology, TopologyResult


def test_empty_edges_returns_none():
    assert build_topology([]) is None


def test_single_edge_creates_two_nodes():
    result = build_topology([("ingest", "transform")])
    assert result is not None
    assert "ingest" in result.nodes
    assert "transform" in result.nodes


def test_upstream_and_downstream_populated():
    result = build_topology([("ingest", "transform"), ("transform", "load")])
    assert "transform" in result.nodes["ingest"].downstream
    assert "ingest" in result.nodes["transform"].upstream


def test_critical_path_is_longest_chain():
    edges = [("a", "b"), ("b", "c"), ("b", "d")]
    result = build_topology(edges)
    assert len(result.critical_path) == 3
    assert result.critical_path[0] == "a"


def test_bottleneck_detected_when_multiple_downstream():
    edges = [("hub", "x"), ("hub", "y")]
    result = build_topology(edges)
    assert "hub" in result.bottlenecks


def test_no_bottleneck_for_linear_chain():
    edges = [("a", "b"), ("b", "c")]
    result = build_topology(edges)
    assert result.bottlenecks == []


def test_to_dict_contains_expected_keys():
    result = build_topology([("a", "b")])
    d = result.to_dict()
    assert "nodes" in d
    assert "critical_path" in d
    assert "bottlenecks" in d


def test_disconnected_roots_both_explored():
    edges = [("a", "b"), ("c", "d"), ("d", "e")]
    result = build_topology(edges)
    # longest path is c->d->e (length 3)
    assert len(result.critical_path) == 3
    assert result.critical_path[0] == "c"
