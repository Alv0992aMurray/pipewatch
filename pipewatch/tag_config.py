"""Load tag filter definitions from YAML configuration."""
from __future__ import annotations

import os
from typing import Any

import yaml

from pipewatch.tag_filter import TagFilter

_DEFAULT_CONFIG = "pipewatch_tags.yml"


class TagConfigError(Exception):
    """Raised when a tag configuration file is invalid."""


def _parse_filter(raw: Any, index: int) -> TagFilter:
    if not isinstance(raw, dict):
        raise TagConfigError(f"Tag filter at index {index} must be a mapping.")
    required = raw.get("required", {})
    if not isinstance(required, dict):
        raise TagConfigError(
            f"Tag filter at index {index}: 'required' must be a key/value mapping."
        )
    return TagFilter(required={str(k): str(v) for k, v in required.items()})


def load_tag_filters(path: str | None = None) -> list[TagFilter]:
    """Load tag filters from *path* (defaults to ``pipewatch_tags.yml``)."""
    config_path = path or os.environ.get("PIPEWATCH_TAGS_CONFIG", _DEFAULT_CONFIG)
    if not os.path.exists(config_path):
        raise TagConfigError(f"Tag config file not found: {config_path}")
    with open(config_path) as fh:
        data = yaml.safe_load(fh)
    if data is None:
        return []
    if not isinstance(data, dict):
        raise TagConfigError("Tag config must be a YAML mapping.")
    raw_filters = data.get("tag_filters", [])
    if not isinstance(raw_filters, list):
        raise TagConfigError("'tag_filters' must be a list.")
    return [_parse_filter(item, i) for i, item in enumerate(raw_filters)]
