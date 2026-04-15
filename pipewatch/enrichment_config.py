"""Load enrichment rules from YAML configuration."""
from __future__ import annotations

from pathlib import Path
from typing import List

import yaml

from pipewatch.enrichment import EnrichmentRule


class EnrichmentConfigError(Exception):
    pass


def _parse_rule(raw: dict) -> EnrichmentRule:
    if "metadata" not in raw:
        raise EnrichmentConfigError("Enrichment rule missing required 'metadata' key")
    metadata = raw["metadata"]
    if not isinstance(metadata, dict):
        raise EnrichmentConfigError("'metadata' must be a mapping of key/value strings")
    return EnrichmentRule(
        metadata={str(k): str(v) for k, v in metadata.items()},
        pipeline=raw.get("pipeline"),
        severity=raw.get("severity"),
    )


def load_enrichment_rules(path: str = "pipewatch_enrichment.yml") -> List[EnrichmentRule]:
    p = Path(path)
    if not p.exists():
        return []
    text = p.read_text()
    if not text.strip():
        return []
    data = yaml.safe_load(text)
    if not isinstance(data, dict):
        return []
    raw_rules = data.get("enrichments", [])
    if not raw_rules:
        return []
    return [_parse_rule(r) for r in raw_rules]
