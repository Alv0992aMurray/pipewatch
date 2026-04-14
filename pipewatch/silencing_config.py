"""Load silence windows from YAML configuration."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import List

import yaml

from pipewatch.silencing import SilenceWindow

_DEFAULT_PATH = Path("pipewatch_silences.yml")


class SilencingConfigError(Exception):
    pass


def _parse_window(raw: dict) -> SilenceWindow:
    for key in ("name", "pipeline", "start", "end"):
        if key not in raw:
            raise SilencingConfigError(f"Silence window missing required field: '{key}'")
    try:
        start = datetime.fromisoformat(raw["start"])
        end = datetime.fromisoformat(raw["end"])
    except ValueError as exc:
        raise SilencingConfigError(f"Invalid datetime in silence window: {exc}") from exc
    return SilenceWindow(
        name=raw["name"],
        pipeline=raw["pipeline"],
        start=start,
        end=end,
        reason=raw.get("reason", ""),
    )


def load_silence_windows(path: Path = _DEFAULT_PATH) -> List[SilenceWindow]:
    """Load silence windows from *path*; returns empty list if file absent."""
    if not path.exists():
        return []
    with open(path) as fh:
        data = yaml.safe_load(fh)
    if not data:
        return []
    raw_windows = data.get("silences", [])
    return [_parse_window(r) for r in raw_windows]
