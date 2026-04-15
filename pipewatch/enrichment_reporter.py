"""Format enrichment results for CLI output and JSON export."""
from __future__ import annotations

import json
from typing import List

from pipewatch.enrichment import EnrichedAlert, EnrichmentResult


def format_enriched_alert(ea: EnrichedAlert) -> str:
    severity = ea.alert.severity.value.upper()
    parts = [f"[{severity}] {ea.alert.pipeline} — {ea.alert.message}"]
    if ea.metadata:
        for k, v in sorted(ea.metadata.items()):
            parts.append(f"    {k}: {v}")
    return "\n".join(parts)


def format_enrichment_report(result: EnrichmentResult) -> str:
    if not result.enriched:
        return "No enriched alerts."
    lines: List[str] = [f"Enriched Alerts ({result.total}):", ""]
    for ea in result.enriched:
        lines.append(format_enriched_alert(ea))
        lines.append("")
    return "\n".join(lines).rstrip()


def enrichment_report_to_json(result: EnrichmentResult) -> str:
    return json.dumps([ea.to_dict() for ea in result.enriched], indent=2)
