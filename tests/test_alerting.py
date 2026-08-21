"""Tests for quality/alerting.py. Mocks the HTTP call — no real network/server needed."""

from unittest.mock import patch

import pytest
import requests

from quality.alerting import Alert, send_alert


def test_send_alert_success():
    with patch("quality.alerting.requests.post") as mock_post:
        mock_post.return_value.raise_for_status.return_value = None
        alert = Alert(severity="error", source="test_gate", summary="something broke")
        result = send_alert(alert, webhook_url="http://example.invalid/alert")

    assert result is True
    mock_post.assert_called_once()
    _, kwargs = mock_post.call_args
    assert kwargs["json"]["summary"] == "something broke"
    assert kwargs["json"]["severity"] == "error"


def test_send_alert_never_raises_on_failure():
    with patch("quality.alerting.requests.post", side_effect=requests.ConnectionError("refused")):
        alert = Alert(severity="error", source="test_gate", summary="something broke")
        result = send_alert(alert, webhook_url="http://example.invalid/alert")

    assert result is False  # failure is reported via return value, not an exception


def test_alert_defaults_details_to_empty_dict():
    alert = Alert(severity="warning", source="x", summary="y")
    assert alert.details == {}
    assert alert.triggered_at is not None
