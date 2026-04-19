"""Human-readable and JSON reporters for cascade failure results."""
from __future__ import annotations

import json
from typing import List

from pipewatch.cascade import CascadeResult, CascadeLink


def format_cascade_link(link: CascadeLink) -> str:
    return (
        f"  ⚡ {link.upstream} → {link.downstream} "
        f"({link.overlap_count} co-failure(s) within {link.window_seconds}s)"
    )


def format_cascade_report(result: CascadeResult) -> str:
    lines: List[str] = ["=== Cascade Failure Report ==="]

    if not result.links:
        lines.append("  No cascade links detected.")
        return "\n".join(lines)

    lines.append(f"  {result.total_links} cascade link(s) found:")
    for link in result.links:
        lines.append(format_cascade_link(link))

    affected = result.affected_pipelines()
    lines.append(f"  Affected pipelines: {', '.join(affected)}")

    return "\n".join(lines)


def cascade_report_to_json(result: CascadeResult) -> str:
    return json.dumps(result.to_dict(), indent=2)
