"""Tests for pipewatch.schedule_config."""

from pathlib import Path

import pytest

from pipewatch.schedule_config import ScheduleConfigError, load_schedules


def write_yaml(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "schedules.yml"
    p.write_text(content)
    return p


def test_load_schedules_basic(tmp_path):
    p = write_yaml(tmp_path, """
schedules:
  - pipeline: orders
    interval_minutes: 60
  - pipeline: inventory
    interval_minutes: 30
    grace_period_minutes: 10
""")
    configs = load_schedules(p)
    assert len(configs) == 2
    assert configs[0].pipeline_name == "orders"
    assert configs[0].interval_minutes == 60
    assert configs[0].grace_period_minutes == 5  # default
    assert configs[1].pipeline_name == "inventory"
    assert configs[1].grace_period_minutes == 10


def test_load_schedules_missing_file(tmp_path):
    result = load_schedules(tmp_path / "nonexistent.yml")
    assert result == []


def test_load_schedules_empty_file(tmp_path):
    p = write_yaml(tmp_path, "")
    assert load_schedules(p) == []


def test_load_schedules_no_schedules_key(tmp_path):
    p = write_yaml(tmp_path, "rules:\n  - foo")
    assert load_schedules(p) == []


def test_missing_pipeline_name_raises(tmp_path):
    p = write_yaml(tmp_path, """
schedules:
  - interval_minutes: 60
""")
    with pytest.raises(ScheduleConfigError, match="pipeline"):
        load_schedules(p)


def test_missing_interval_raises(tmp_path):
    p = write_yaml(tmp_path, """
schedules:
  - pipeline: orders
""")
    with pytest.raises(ScheduleConfigError, match="interval_minutes"):
        load_schedules(p)


def test_invalid_interval_type_raises(tmp_path):
    p = write_yaml(tmp_path, """
schedules:
  - pipeline: orders
    interval_minutes: "hourly"
""")
    with pytest.raises(ScheduleConfigError, match="positive integer"):
        load_schedules(p)


def test_negative_interval_raises(tmp_path):
    p = write_yaml(tmp_path, """
schedules:
  - pipeline: orders
    interval_minutes: -10
""")
    with pytest.raises(ScheduleConfigError, match="positive integer"):
        load_schedules(p)
