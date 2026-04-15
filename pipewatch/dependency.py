"""Pipeline dependency tracking and upstream health propagation."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric, is_healthy


@dataclass
class DependencyEdge:
    upstream: str
    downstream: str

    def to_dict(self) -> dict:
        return {"upstream": self.upstream, "downstream": self.downstream}


@dataclass
class DependencyResult:
    pipeline: str
    upstream_pipelines: List[str]
    blocked_by: List[str]  # unhealthy upstreams
    is_blocked: bool
    message: str

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "upstream_pipelines": self.upstream_pipelines,
            "blocked_by": self.blocked_by,
            "is_blocked": self.is_blocked,
            "message": self.message,
        }


def build_graph(edges: List[DependencyEdge]) -> Dict[str, List[str]]:
    """Return a mapping of downstream -> list of upstream pipeline names."""
    graph: Dict[str, List[str]] = {}
    for edge in edges:
        graph.setdefault(edge.downstream, []).append(edge.upstream)
    return graph


def check_dependencies(
    pipeline: str,
    metrics: Dict[str, PipelineMetric],
    graph: Dict[str, List[str]],
) -> Optional[DependencyResult]:
    """Evaluate whether *pipeline* is blocked by an unhealthy upstream.

    Returns ``None`` when the pipeline has no registered upstreams.
    """
    upstreams = graph.get(pipeline)
    if not upstreams:
        return None

    blocked_by: List[str] = []
    for up in upstreams:
        metric = metrics.get(up)
        if metric is not None and not is_healthy(metric):
            blocked_by.append(up)

    is_blocked = len(blocked_by) > 0
    if is_blocked:
        msg = f"{pipeline} is blocked by unhealthy upstream(s): {', '.join(blocked_by)}"
    else:
        msg = f"{pipeline} — all {len(upstreams)} upstream(s) healthy"

    return DependencyResult(
        pipeline=pipeline,
        upstream_pipelines=list(upstreams),
        blocked_by=blocked_by,
        is_blocked=is_blocked,
        message=msg,
    )


def check_all_dependencies(
    metrics: Dict[str, PipelineMetric],
    graph: Dict[str, List[str]],
) -> List[DependencyResult]:
    """Run dependency checks for every downstream pipeline in *graph*."""
    results: List[DependencyResult] = []
    for pipeline in graph:
        result = check_dependencies(pipeline, metrics, graph)
        if result is not None:
            results.append(result)
    return results
