"""Webhook notification support for pipewatch alerts."""
from __future__ import annotations

import json
import urllib.request
import urllib.error
from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.alerts import Alert, AlertSeverity


@dataclass
class WebhookConfig:
    url: str
    min_severity: AlertSeverity = AlertSeverity.WARNING
    headers: dict = field(default_factory=dict)
    timeout: int = 10


@dataclass
class WebhookResult:
    url: str
    success: bool
    status_code: Optional[int] = None
    error: Optional[str] = None


def _build_payload(alerts: List[Alert]) -> bytes:
    data = [
        {
            "pipeline": a.pipeline,
            "metric": a.metric,
            "severity": a.severity.value,
            "message": a.message,
            "value": a.value,
            "threshold": a.threshold,
        }
        for a in alerts
    ]
    return json.dumps({"alerts": data}).encode("utf-8")


def _filter_alerts(alerts: List[Alert], min_severity: AlertSeverity) -> List[Alert]:
    order = [AlertSeverity.WARNING, AlertSeverity.CRITICAL]
    min_idx = order.index(min_severity)
    return [a for a in alerts if order.index(a.severity) >= min_idx]


def send_webhook(config: WebhookConfig, alerts: List[Alert]) -> WebhookResult:
    """POST filtered alerts to the configured webhook URL."""
    relevant = _filter_alerts(alerts, config.min_severity)
    if not relevant:
        return WebhookResult(url=config.url, success=True, status_code=None)

    payload = _build_payload(relevant)
    headers = {"Content-Type": "application/json", **config.headers}
    req = urllib.request.Request(config.url, data=payload, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=config.timeout) as resp:
            return WebhookResult(url=config.url, success=True, status_code=resp.status)
    except urllib.error.HTTPError as exc:
        return WebhookResult(url=config.url, success=False, status_code=exc.code, error=str(exc))
    except Exception as exc:  # noqa: BLE001
        return WebhookResult(url=config.url, success=False, error=str(exc))
