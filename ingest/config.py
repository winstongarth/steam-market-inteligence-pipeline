"""Central config for tunable [CONFIG] values. Override via environment variables."""

from __future__ import annotations

import os


def _float_env(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


def _int_env(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


# Measured: first 429 at ~2 req/s sustained, recovered after ~30s.
# Production default is set to 25% of that measured limit (under a 40% ceiling).
# See docs/FINDINGS.md and docs/DECISIONS.md for the measurement.
GLOBAL_REQUESTS_PER_SECOND = _float_env("STEAM_RPS", 0.5)

# On a 429, cut the refill rate for the rest of the hour by this factor.
BACKOFF_REFILL_REDUCTION_FACTOR = _float_env("STEAM_BACKOFF_REDUCTION", 0.5)

# Jittered exponential backoff for retrying after a 429.
RETRY_BASE_DELAY_SECONDS = _float_env("STEAM_RETRY_BASE_DELAY", 2.0)
RETRY_MAX_DELAY_SECONDS = _float_env("STEAM_RETRY_MAX_DELAY", 60.0)
RETRY_JITTER_SECONDS = _float_env("STEAM_RETRY_JITTER", 1.0)

# Circuit breaker.
CIRCUIT_BREAKER_CONSECUTIVE_429_THRESHOLD = _int_env("STEAM_CB_429_THRESHOLD", 3)
CIRCUIT_BREAKER_429_HALT_SECONDS = _float_env("STEAM_CB_429_HALT", 30 * 60)
CIRCUIT_BREAKER_403_HALT_SECONDS = _float_env("STEAM_CB_403_HALT", 30 * 60)
CIRCUIT_BREAKER_BAN_HALT_SECONDS = _float_env("STEAM_CB_BAN_HALT", 6 * 60 * 60)

USER_AGENT = os.environ.get(
    "STEAM_USER_AGENT",
    "steam-market-pipeline/0.1 (personal research project; "
    "contact: winstonpatrickgarth@gmail.com)",
)

REQUEST_TIMEOUT_SECONDS = _float_env("STEAM_REQUEST_TIMEOUT", 15.0)

# Tier cadences.
TIER_A_INTERVAL_SECONDS = _int_env("STEAM_TIER_A_INTERVAL", 5 * 60)
TIER_B_INTERVAL_SECONDS = _int_env("STEAM_TIER_B_INTERVAL", 60 * 60)
TIER_C_INTERVAL_SECONDS = _int_env("STEAM_TIER_C_INTERVAL", 24 * 60 * 60)

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
KAFKA_RAW_TOPIC = "market.raw.v1"

# Games to cover. Multi-app coverage is deliberate (heterogeneous schemas).
APP_IDS: dict[int, str] = {
    730: "CS2",
    570: "Dota 2",
    440: "TF2",
    252490: "Rust",
    578080: "PUBG",
}

S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL", "http://localhost:9000")
S3_BUCKET = os.environ.get("S3_BUCKET", "steam-lake")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "minioadmin")
