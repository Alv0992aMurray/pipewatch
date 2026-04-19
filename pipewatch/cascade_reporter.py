"""Human-readable and JSON reporters for cascade failure results."""
from __future__ import annotations

import json
from typing import List

from pipewatch.cascade import CascadeResult, CascadeLink


def format_cascade_link(link: CascadeLink) -> str:
    """Format a single cascade link as a human-readable string."""
    return (
        f"  ⚡ {link.upstream} → {link.downstream} "
        f"({link.overlap_count} co-failure(s) within {link.window_seconds}s)"
    )


def format_cascade_report(result: CascadeResult) -> str:
    """Format a full cascade result as a human-readable report."""
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
    """Serialize a cascade result to a JSON string."""
    return json.dumps(result.to_dict(), indent=2)


def print_cascade_report(result: CascadeResult, use_json: bool = False) -> None:
    """Print a cascade report to stdout in either human-readable or JSON format.

    Args:
        result: The cascade analysis result to report.
        use_json: If True, output JSON instead of the human-readable format.
    """
    if use_json:
        print(cascade_report_to_json(result))
    else:
        print(format_cascade_report(result))
