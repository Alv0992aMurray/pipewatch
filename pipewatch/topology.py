"""Pipeline topology analysis — detects critical paths and bottlenecks."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class TopologyNode:
    pipeline: str
    upstream: List[str] = field(default_factory=list)
    downstream: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "upstream": self.upstream,
            "downstream": self.downstream,
        }


@dataclass
class TopologyResult:
    nodes: Dict[str, TopologyNode]
    critical_path: List[str]
    bottlenecks: List[str]

    def to_dict(self) -> dict:
        return {
            "nodes": {k: v.to_dict() for k, v in self.nodes.items()},
            "critical_path": self.critical_path,
            "bottlenecks": self.bottlenecks,
        }


def build_topology(edges: List[tuple[str, str]]) -> Optional[TopologyResult]:
    """Build a topology from (upstream, downstream) edge pairs."""
    if not edges:
        return None

    nodes: Dict[str, TopologyNode] = {}

    for up, down in edges:
        if up not in nodes:
            nodes[up] = TopologyNode(pipeline=up)
        if down not in nodes:
            nodes[down] = TopologyNode(pipeline=down)
        nodes[up].downstream.append(down)
        nodes[down].upstream.append(up)

    critical_path = _longest_path(nodes)
    bottlenecks = [
        name for name, node in nodes.items()
        if len(node.downstream) >= 2
    ]

    return TopologyResult(nodes=nodes, critical_path=critical_path, bottlenecks=bottlenecks)


def _longest_path(nodes: Dict[str, TopologyNode]) -> List[str]:
    """Return the longest chain from any root to any leaf."""
    roots = [n for n, node in nodes.items() if not node.upstream]
    best: List[str] = []

    def dfs(current: str, path: List[str]) -> None:
        nonlocal best
        path = path + [current]
        if len(path) > len(best):
            best = path
        for child in nodes[current].downstream:
            dfs(child, path)

    for root in roots:
        dfs(root, [])

    return best
