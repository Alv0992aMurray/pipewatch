"""Render a Digest as human-readable text or JSON."""

import json
from typing import List

from pipewatch.digest import Digest, DigestEntry
from pipewatch.reporter import _severity_label


def _format_entry(entry: DigestEntry) -> str:
    lines: List[str] = []
    lines.append(f"Pipeline : {entry.pipeline_name}")
    lines.append(f"Snapshots: {entry.snapshot_count}")

    if entry.latest_success_rate is not None:
        lines.append(f"Success  : {entry.latest_success_rate:.1%}")
    else:
        lines.append("Success  : n/a")

    if entry.latest_throughput is not None:
        lines.append(f"Throughput: {entry.latest_throughput:.1f} rows/s")
    else:
        lines.append("Throughput: n/a")

    trend = entry.trend
    lines.append(f"Trend    : {trend.summary_line()}")

    if entry.alerts:
        lines.append("Alerts:")
        for alert in entry.alerts:
            label = _severity_label(alert.severity)
            lines.append(f"  [{label}] {alert.message}")
    else:
        lines.append("Alerts   : none")

    return "\n".join(lines)


def format_digest(digest: Digest) -> str:
    """Return a multi-pipeline digest as a formatted string."""
    if not digest.entries:
        return "No pipeline data available."

    sections = [_format_entry(e) for e in digest.entries]
    separator = "\n" + "-" * 40 + "\n"
    header = f"=== PipeWatch Digest ({len(digest.entries)} pipeline(s)) ==="
    footer = f"Total alerts: {digest.total_alerts()}"
    return "\n".join([header, separator.join(sections), footer])


def digest_to_json(digest: Digest) -> str:
    """Serialise a Digest to a JSON string."""
    payload = {
        "pipelines": [
            {
                "pipeline_name": e.pipeline_name,
                "snapshot_count": e.snapshot_count,
                "latest_success_rate": e.latest_success_rate,
                "latest_throughput": e.latest_throughput,
                "trend": {
                    "direction": e.trend.direction,
                    "slope": e.trend.slope,
                    "summary": e.trend.summary_line(),
                },
                "alerts": [
                    {"severity": a.severity.value, "message": a.message}
                    for a in e.alerts
                ],
            }
            for e in digest.entries
        ],
        "total_alerts": digest.total_alerts(),
    }
    return json.dumps(payload, indent=2)
