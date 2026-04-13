"""Load webhook notification configs from YAML."""
from __future__ import annotations

import os
from typing import List

import yaml

from pipewatch.alerts import AlertSeverity
from pipewatch.webhook import WebhookConfig

_DEFAULT_PATH = "pipewatch_webhooks.yml"


class WebhookConfigError(Exception):
    pass


def _parse_webhook(raw: dict) -> WebhookConfig:
    if "url" not in raw:
        raise WebhookConfigError("Each webhook entry must have a 'url' field.")
    url = raw["url"]
    severity_str = raw.get("min_severity", "warning").upper()
    try:
        min_severity = AlertSeverity[severity_str]
    except KeyError:
        raise WebhookConfigError(f"Unknown severity: {severity_str}")
    headers = raw.get("headers", {})
    timeout = int(raw.get("timeout", 10))
    return WebhookConfig(url=url, min_severity=min_severity, headers=headers, timeout=timeout)


def load_webhook_configs(path: str = _DEFAULT_PATH) -> List[WebhookConfig]:
    """Load webhook configs from a YAML file."""
    resolved = os.path.abspath(path)
    if not os.path.exists(resolved):
        return []
    with open(resolved) as fh:
        data = yaml.safe_load(fh)
    if not data:
        return []
    entries = data.get("webhooks", [])
    if not isinstance(entries, list):
        raise WebhookConfigError("'webhooks' must be a list.")
    return [_parse_webhook(entry) for entry in entries]
