"""Format clustering results for CLI output and JSON export."""
from __future__ import annotations
import json
from typing import List
from pipewatch.clustering import ClusteringResult, Cluster

_ICONS = {"healthy": "✅", "degraded": "⚠️", "failing": "❌"}


def _fmt_pct(value: float) -> str:
    return f"{value * 100:.1f}%"


def format_cluster(cluster: Cluster) -> str:
    icon = _ICONS.get(cluster.label, "•")
    lines = [
        f"{icon} [{cluster.label.upper()}] — {cluster.count} pipeline(s), "
        f"avg success: {_fmt_pct(cluster.avg_success_rate)}"
    ]
    for entry in cluster.entries:
        lines.append(
            f"   - {entry.pipeline}: success={_fmt_pct(entry.success_rate)}, "
            f"errors={_fmt_pct(entry.error_rate)}"
        )
    return "\n".join(lines)


def format_clustering_report(result: ClusteringResult) -> str:
    if result.total_pipelines == 0:
        return "No pipelines to cluster."
    lines = [f"Pipeline Clustering — {result.total_pipelines} total pipeline(s)\n"]
    for cluster in result.clusters:
        if cluster.count > 0:
            lines.append(format_cluster(cluster))
    return "\n".join(lines)


def clustering_report_to_json(result: ClusteringResult) -> str:
    return json.dumps(result.to_dict(), indent=2)
