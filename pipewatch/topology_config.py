"""Load topology edge definitions from YAML config."""
from __future__ import annotations
from pathlib import Path
from typing import List, Tuple
import yaml


class TopologyConfigError(Exception):
    pass


def _parse_edge(raw: dict) -> Tuple[str, str]:
    if "upstream" not in raw or "downstream" not in raw:
        raise TopologyConfigError(f"Edge missing 'upstream' or 'downstream': {raw}")
    return str(raw["upstream"]), str(raw["downstream"])


def load_topology_edges(path: str = "pipewatch_topology.yml") -> List[Tuple[str, str]]:
    p = Path(path)
    if not p.exists():
        return []
    raw = yaml.safe_load(p.read_text()) or {}
    edges_raw = raw.get("edges", [])
    if not edges_raw:
        return []
    return [_parse_edge(e) for e in edges_raw]
