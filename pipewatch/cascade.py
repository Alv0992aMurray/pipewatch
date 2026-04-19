"""Cascade failure detection — identifies pipelines whose failures
correlate with upstream failures within a time window."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from pipewatch.history import PipelineHistory


@dataclass
class CascadeLink:
    upstream: str
    downstream: str
    overlap_count: int
    window_seconds: int

    def to_dict(self) -> dict:
        return {
            "upstream": self.upstream,
            "downstream": self.downstream,
            "overlap_count": self.overlap_count,
            "window_seconds": self.window_seconds,
        }


@dataclass
class CascadeResult:
    links: List[CascadeLink] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {"links": [l.to_dict() for l in self.links]}

    @property
    def total_links(self) -> int:
        return len(self.links)

    def affected_pipelines(self) -> List[str]:
        seen = set()
        result = []
        for lnk in self.links:
            for name in (lnk.upstream, lnk.downstream):
                if name not in seen:
                    seen.add(name)
                    result.append(name)
        return result


def _failure_times(history: PipelineHistory) -> List[datetime]:
    return [
        snap.timestamp
        for snap in history.snapshots
        if not snap.healthy
    ]


def detect_cascade(
    histories: Dict[str, PipelineHistory],
    edges: List[tuple],
    window_seconds: int = 300,
) -> CascadeResult:
    """For each (upstream, downstream) edge, count how many upstream
    failures are followed by a downstream failure within *window_seconds*."""
    links: List[CascadeLink] = []
    window = timedelta(seconds=window_seconds)

    for upstream_name, downstream_name in edges:
        up_hist = histories.get(upstream_name)
        dn_hist = histories.get(downstream_name)
        if up_hist is None or dn_hist is None:
            continue

        up_failures = _failure_times(up_hist)
        dn_failures = _failure_times(dn_hist)

        if not up_failures or not dn_failures:
            continue

        overlap = 0
        for up_ts in up_failures:
            for dn_ts in dn_failures:
                if timedelta(0) <= (dn_ts - up_ts) <= window:
                    overlap += 1
                    break

        if overlap > 0:
            links.append(CascadeLink(
                upstream=upstream_name,
                downstream=downstream_name,
                overlap_count=overlap,
                window_seconds=window_seconds,
            ))

    return CascadeResult(links=links)
