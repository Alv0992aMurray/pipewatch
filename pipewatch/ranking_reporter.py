"""Reporters for pipeline ranking results."""
from __future__ import annotations

import json
from typing import Optional

from pipewatch.ranking import RankEntry, RankingResult


def _icon(healthy: bool) -> str:
    return "✅" if healthy else "❌"


def format_rank_entry(entry: RankEntry) -> str:
    return (
        f"  #{entry.rank:<3} {entry.pipeline:<30} "
        f"score={entry.score:.3f}  "
        f"sr={entry.success_rate:.1%}  "
        f"tp={entry.throughput:.0f}  "
        f"{_icon(entry.healthy)}"
    )


def format_ranking_report(
    result: RankingResult,
    limit: Optional[int] = None,
) -> str:
    if not result.entries:
        return "Pipeline Rankings\n  (no pipelines to rank)"

    entries = result.entries if limit is None else result.entries[:limit]
    lines = ["Pipeline Rankings", "-" * 60]
    lines.extend(format_rank_entry(e) for e in entries)
    lines.append(f"\nTotal pipelines ranked: {len(result.entries)}")
    return "\n".join(lines)


def ranking_report_to_json(result: RankingResult) -> str:
    return json.dumps(result.to_dict(), indent=2)
