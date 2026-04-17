"""Reachability analysis: determine which pipelines are reachable from a given source."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set


@dataclass
class ReachabilityResult:
    source: str
    reachable: List[str]
    unreachable: List[str]
    depth_map: Dict[str, int]

    def to_dict(self) -> dict:
        return {
            "source": self.source,
            "reachable": self.reachable,
            "unreachable": self.unreachable,
            "depth_map": self.depth_map,
        }

    @property
    def total_reachable(self) -> int:
        return len(self.reachable)


def _bfs(
    source: str,
    graph: Dict[str, List[str]],
) -> Dict[str, int]:
    """Return a mapping of node -> depth reachable from source via BFS."""
    visited: Dict[str, int] = {source: 0}
    queue = [source]
    while queue:
        current = queue.pop(0)
        for neighbour in graph.get(current, []):
            if neighbour not in visited:
                visited[neighbour] = visited[current] + 1
                queue.append(neighbour)
    return visited


def analyse_reachability(
    source: str,
    graph: Dict[str, List[str]],
    all_nodes: Optional[List[str]] = None,
) -> ReachabilityResult:
    """Analyse which nodes in *graph* are reachable from *source*.

    Args:
        source: The pipeline name to start from.
        graph: Adjacency list {pipeline: [downstream_pipelines]}.
        all_nodes: Optional explicit list of all known nodes.
            If omitted, nodes are inferred from the graph keys and values.
    """
    if all_nodes is None:
        known: Set[str] = set(graph.keys())
        for targets in graph.values():
            known.update(targets)
        all_nodes = sorted(known)

    depth_map = _bfs(source, graph)
    reachable = sorted(n for n in depth_map if n != source)
    unreachable = sorted(n for n in all_nodes if n not in depth_map)

    return ReachabilityResult(
        source=source,
        reachable=reachable,
        unreachable=unreachable,
        depth_map={k: v for k, v in depth_map.items() if k != source},
    )
