"""Load maintenance windows from YAML configuration."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import List

import yaml

from pipewatch.maintenance import MaintenanceWindow


class MaintenanceConfigError(Exception):
    pass


def _parse_window(raw: dict) -> MaintenanceWindow:
    try:
        pipeline = raw.get("pipeline", "*")
        start = datetime.fromisoformat(raw["start"])
        end = datetime.fromisoformat(raw["end"])
        reason = raw.get("reason", "")
        return MaintenanceWindow(pipeline=pipeline, start=start, end=end, reason=reason)
    except KeyError as exc:
        raise MaintenanceConfigError(f"Missing field in maintenance window: {exc}") from exc
    except ValueError as exc:
        raise MaintenanceConfigError(f"Invalid datetime in maintenance window: {exc}") from exc


def load_maintenance_windows(path: str = "pipewatch_maintenance.yml") -> List[MaintenanceWindow]:
    p = Path(path)
    if not p.exists():
        return []
    with p.open() as fh:
        data = yaml.safe_load(fh)
    if not data:
        return []
    raw_list = data.get("maintenance", [])
    if not raw_list:
        return []
    return [_parse_window(r) for r in raw_list]
