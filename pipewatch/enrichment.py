"""Alert enrichment: attach contextual metadata to alerts before routing/reporting."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.alerts import Alert


@dataclass
class EnrichmentRule:
    """Adds key/value metadata to alerts matching a pipeline and/or severity."""
    metadata: Dict[str, str]
    pipeline: Optional[str] = None          # None means match any pipeline
    severity: Optional[str] = None          # None means match any severity

    def matches(self, alert: Alert) -> bool:
        if self.pipeline and alert.pipeline != self.pipeline:
            return False
        if self.severity and alert.severity.value != self.severity:
            return False
        return True


@dataclass
class EnrichedAlert:
    alert: Alert
    metadata: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict:
        base = {
            "pipeline": self.alert.pipeline,
            "rule": self.alert.rule,
            "severity": self.alert.severity.value,
            "message": self.alert.message,
            "value": self.alert.value,
        }
        if self.metadata:
            base["metadata"] = dict(self.metadata)
        return base


@dataclass
class EnrichmentResult:
    enriched: List[EnrichedAlert] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.enriched)


def enrich_alerts(
    alerts: List[Alert],
    rules: List[EnrichmentRule],
) -> EnrichmentResult:
    """Apply all matching enrichment rules to each alert."""
    enriched: List[EnrichedAlert] = []
    for alert in alerts:
        merged: Dict[str, str] = {}
        for rule in rules:
            if rule.matches(alert):
                merged.update(rule.metadata)
        enriched.append(EnrichedAlert(alert=alert, metadata=merged))
    return EnrichmentResult(enriched=enriched)
