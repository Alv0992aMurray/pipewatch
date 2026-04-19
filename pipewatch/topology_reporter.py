"""Reporters for topology analysis results."""
from __future__ import annotations
import json
from typing import Optional
from pipewatch.topology import TopologyResult


def format_topology_result(result: Optional[TopologyResult]) -> str:
    if result is None:
        return "  [topology] no edges defined\n"

    lines = ["  [topology]"]
    lines.append(f"    pipelines : {len(result.nodes)}")

    if result.critical_path:
        lines.append("    critical path : " + " → ".join(result.critical_path))
    else:
        lines.append("    critical path : (none)")

    if result.bottlenecks:
        lines.append("    bottlenecks   : " + ", ".join(result.bottlenecks))
    else:
        lines.append("    bottlenecks   : (none)")

    return "\n".join(lines) + "\n"


def format_topology_report(results: list[Optional[TopologyResult]]) -> str:
    if not results:
        return "no topology data\n"
    return "".join(format_topology_result(r) for r in results)


def topology_report_to_json(result: Optional[TopologyResult]) -> str:
    if result is None:
        return json.dumps({"topology": None})
    return json.dumps({"topology": result.to_dict()}, indent=2)
