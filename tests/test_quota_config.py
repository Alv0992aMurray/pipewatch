"""Tests for pipewatch.quota_config."""
from pathlib import Path

import pytest

from pipewatch.quota_config import QuotaConfigError, load_quota_configs


def _write(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "quotas.yml"
    p.write_text(content)
    return p


def test_missing_file_returns_empty_list(tmp_path):
    result = load_quota_configs(tmp_path / "missing.yml")
    assert result == []


def test_empty_file_returns_empty_list(tmp_path):
    p = _write(tmp_path, "")
    assert load_quota_configs(p) == []


def test_no_quotas_key_returns_empty_list(tmp_path):
    p = _write(tmp_path, "other: []")
    assert load_quota_configs(p) == []


def test_load_basic_quota(tmp_path):
    p = _write(tmp_path, "quotas:\n  - pipeline: etl\n    max_alerts: 5\n")
    result = load_quota_configs(p)
    assert len(result) == 1
    assert result[0].pipeline == "etl"
    assert result[0].max_alerts == 5
    assert result[0].window_minutes == 60


def test_load_custom_window(tmp_path):
    p = _write(tmp_path, "quotas:\n  - pipeline: etl\n    max_alerts: 10\n    window_minutes: 30\n")
    result = load_quota_configs(p)
    assert result[0].window_minutes == 30


def test_missing_pipeline_raises(tmp_path):
    p = _write(tmp_path, "quotas:\n  - max_alerts: 5\n")
    with pytest.raises(QuotaConfigError, match="pipeline"):
        load_quota_configs(p)


def test_missing_max_alerts_raises(tmp_path):
    p = _write(tmp_path, "quotas:\n  - pipeline: etl\n")
    with pytest.raises(QuotaConfigError, match="max_alerts"):
        load_quota_configs(p)


def test_load_multiple_quotas(tmp_path):
    content = "quotas:\n  - pipeline: etl\n    max_alerts: 3\n  - pipeline: ingest\n    max_alerts: 10\n"
    p = _write(tmp_path, content)
    result = load_quota_configs(p)
    assert len(result) == 2
    assert result[1].pipeline == "ingest"
