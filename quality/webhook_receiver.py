"""Minimal local webhook receiver — a local webhook, staying off AWS. Not a production
alerting system — proves the alerting interface really goes
somewhere: every POSTed alert is appended, one JSON line per alert, to logs/alerts.jsonl.
Swap ALERT_WEBHOOK_URL (quality/alerting.py) for a real Slack/PagerDuty endpoint later;
send_alert()'s callers don't change.

Zero extra dependencies on purpose — stdlib http.server is enough for a local demo
receiver; no need to pull in Flask/FastAPI for this.

Run: uv run python -m quality.webhook_receiver
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

logger = logging.getLogger("quality.webhook_receiver")

ALERTS_LOG_PATH = Path(__file__).resolve().parent.parent / "logs" / "alerts.jsonl"


class AlertHandler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            self.send_response(400)
            self.end_headers()
            return

        ALERTS_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with ALERTS_LOG_PATH.open("a", encoding="utf-8") as f:
            record = {"received_at": datetime.now(timezone.utc).isoformat(), **payload}
            f.write(json.dumps(record) + "\n")

        logger.info("alert received: %s", payload.get("summary"))
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status": "ok"}')

    def log_message(self, format: str, *args: object) -> None:
        pass  # suppress BaseHTTPRequestHandler's default noisy stderr access log


def run(host: str = "0.0.0.0", port: int = 9109) -> None:
    server = HTTPServer((host, port), AlertHandler)
    logger.info("alert webhook receiver listening on %s:%d, logging to %s", host, port, ALERTS_LOG_PATH)
    server.serve_forever()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    run()
