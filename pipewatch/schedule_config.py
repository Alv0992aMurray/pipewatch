"""Load schedule configurations from YAML."""

from __future__ import annotations

from pathlib import Path
from typing import List

try:
    import yaml
except ImportError as exc:  # pragma: no cover
    raise ImportError("PyYAML is required for schedule_config: pip install pyyaml") from exc

from pipewatch.schedule import ScheduleConfig

_DEFAULT_CONFIG_PATH = Path("pipewatch_schedules.yml")


class ScheduleConfigError(ValueError):
    """Raised when a schedule config entry is invalid."""


def _parse_schedule(raw: dict) -> ScheduleConfig:
    """Parse a single schedule entry from a dict."""
    name = raw.get("pipeline")
    if not name:
        raise ScheduleConfigError("Schedule entry missing required field: 'pipeline'")

    interval = raw.get("interval_minutes")
    if interval is None:
        raise ScheduleConfigError(
            f"Schedule entry for '{name}' missing required field: 'interval_minutes'"
        )
    if not isinstance(interval, int) or interval <= 0:
        raise ScheduleConfigError(
            f"'interval_minutes' for '{name}' must be a positive integer, got: {interval!r}"
        )

    grace = raw.get("grace_period_minutes", 5)
    if not isinstance(grace, int) or grace < 0:
        raise ScheduleConfigError(
            f"'grace_period_minutes' for '{name}' must be a non-negative integer, got: {grace!r}"
        )

    return ScheduleConfig(
        pipeline_name=name,
        interval_minutes=interval,
        grace_period_minutes=grace,
    )


def load_schedules(path: Path = _DEFAULT_CONFIG_PATH) -> List[ScheduleConfig]:
    """Load schedule configs from a YAML file."""
    if not path.exists():
        return []

    with path.open() as fh:
        data = yaml.safe_load(fh)

    if not data or "schedules" not in data:
        return []

    return [_parse_schedule(entry) for entry in data["schedules"]]
