"""Tests for pipewatch.enrichment_config."""
from __future__ import annotations

from pathlib import Path

import pytest

from pipewatch.enrichment_config import load_enrichment_rules, EnrichmentConfigError


def _write(tmp_path: Path, content: str) -> str:
    p = tmp_path / "enrichment.yml"
    p.write_text(content)
    return str(p)


def test_missing_file_returns_empty_list(tmp_path):
    rules = load_enrichment_rules(str(tmp_path / "nonexistent.yml"))
    assert rules == []


def test_empty_file_returns_empty_list(tmp_path):
    path = _write(tmp_path, "")
    assert load_enrichment_rules(path) == []


def test_no_enrichments_key_returns_empty_list(tmp_path):
    path = _write(tmp_path, "other_key:\n  - foo\n")
    assert load_enrichment_rules(path) == []


def test_load_basic_enrichment_rule(tmp_path):
    path = _write(tmp_path, """
enrichments:
  - metadata:
      team: data-eng
""")
    rules = load_enrichment_rules(path)
    assert len(rules) == 1
    assert rules[0].metadata == {"team": "data-eng"}
    assert rules[0].pipeline is None
    assert rules[0].severity is None


def test_load_rule_with_pipeline_and_severity(tmp_path):
    path = _write(tmp_path, """
enrichments:
  - pipeline: orders
    severity: critical
    metadata:
      oncall: eng-team
      page: "true"
""")
    rules = load_enrichment_rules(path)
    assert rules[0].pipeline == "orders"
    assert rules[0].severity == "critical"
    assert rules[0].metadata == {"oncall": "eng-team", "page": "true"}


def test_load_multiple_rules(tmp_path):
    path = _write(tmp_path, """
enrichments:
  - metadata:
      team: alpha
  - pipeline: payments
    metadata:
      team: beta
""")
    rules = load_enrichment_rules(path)
    assert len(rules) == 2


def test_missing_metadata_raises_error(tmp_path):
    path = _write(tmp_path, """
enrichments:
  - pipeline: orders
""")
    with pytest.raises(EnrichmentConfigError, match="metadata"):
        load_enrichment_rules(path)


def test_non_dict_metadata_raises_error(tmp_path):
    path = _write(tmp_path, """
enrichments:
  - metadata: "just a string"
""")
    with pytest.raises(EnrichmentConfigError, match="mapping"):
        load_enrichment_rules(path)
