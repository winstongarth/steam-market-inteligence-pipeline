"""Alert routing. Alerts route to a configurable local webhook, defaulting to
quality/webhook_receiver.py's local receiver — this project stays off AWS. The interface
(`send_alert`) is what matters for swapping in a real SNS/Slack/PagerDuty endpoint later;
callers never change.
"""

from __future__ import annotations

import logging
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

import requests

logger = logging.getLogger("quality.alerting")

ALERT_WEBHOOK_URL = os.environ.get("ALERT_WEBHOOK_URL", "http://localhost:9109/alert")


@dataclass
class Alert:
    severity: str  # "error" | "warning"
    source: str  # e.g. "bronze_quality_gate", "dbt_test_staging"
    summary: str
    details: dict[str, Any] = field(default_factory=dict)
    triggered_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


def send_alert(alert: Alert, webhook_url: str = ALERT_WEBHOOK_URL, timeout: float = 5.0) -> bool:
    """POSTs the alert to the configured webhook. Returns True on success, False on
    failure — never raises, since an alert failing to send should not itself crash the
    pipeline (that would turn an observability gap into an outage)."""
    payload = asdict(alert)
    try:
        response = requests.post(webhook_url, json=payload, timeout=timeout)
        response.raise_for_status()
        logger.info("alert sent: %s", alert.summary)
        return True
    except Exception as exc:
        logger.error("failed to send alert to %s: %s. Alert was: %s", webhook_url, exc, payload)
        return False
