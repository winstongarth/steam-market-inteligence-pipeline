"""Token bucket + adaptive backoff + circuit breaker, per CLAUDE.md §2 and §7.1.1.

Non-negotiable rules this module enforces (§2):
- Single well-behaved client: a global token bucket caps sustained request rate.
- Every 429 is a first-class event: logged, and it reduces the bucket's refill rate
  for the rest of the hour rather than just being retried.
- Circuit breaker: 3 consecutive 429s or any 403 halts polling for 30 minutes;
  a ban-shaped response (HTML instead of JSON) halts for 6 hours and requires a
  manual restart (the process must be relaunched — this is deliberately not
  self-healing).
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass, field

from ingest import config

logger = logging.getLogger("ingest.ratelimit")


class CircuitOpenError(RuntimeError):
    """Raised when a caller tries to acquire a token while the breaker is open."""


class ManualRestartRequiredError(RuntimeError):
    """Raised when a ban-shaped response trips the breaker into its hard-halt state."""


@dataclass
class TokenBucket:
    """Async token bucket. One instance is the process-wide global budget.

    Refill rate can be temporarily reduced (never increased above the configured
    ceiling) in response to a 429, and is restored at the top of the next hour.
    """

    rate_per_second: float
    capacity: float | None = None
    _capacity: float = field(init=False)
    _tokens: float = field(init=False)
    _last_refill: float = field(init=False)
    _base_rate: float = field(init=False)
    _current_rate: float = field(init=False)
    _reduced_until_epoch_hour: int | None = field(default=None, init=False)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)

    def __post_init__(self) -> None:
        self._capacity = self.capacity if self.capacity is not None else max(1.0, self.rate_per_second * 2)
        self._base_rate = self.rate_per_second
        self._current_rate = self.rate_per_second
        self._tokens = self._capacity
        self._last_refill = time.monotonic()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self._capacity, self._tokens + elapsed * self._current_rate)
        self._last_refill = now

        current_hour = int(time.time() // 3600)
        if self._reduced_until_epoch_hour is not None and current_hour > self._reduced_until_epoch_hour:
            logger.info(
                "token bucket refill rate restored to %.3f req/s (top of the hour)",
                self._base_rate,
            )
            self._current_rate = self._base_rate
            self._reduced_until_epoch_hour = None

    def reduce_for_rest_of_hour(self, factor: float) -> None:
        """Cut the refill rate for the remainder of the current wall-clock hour."""
        self._current_rate = max(self._current_rate * factor, 0.01)
        self._reduced_until_epoch_hour = int(time.time() // 3600)
        logger.warning(
            "token bucket refill rate reduced to %.3f req/s until top of next hour",
            self._current_rate,
        )

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                self._refill()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                deficit = 1.0 - self._tokens
                wait = deficit / self._current_rate if self._current_rate > 0 else 1.0
            await asyncio.sleep(wait)


@dataclass
class CircuitBreaker:
    """Tracks 429/403/ban-shaped responses and enforces halt windows per §2.7."""

    consecutive_429_threshold: int = config.CIRCUIT_BREAKER_CONSECUTIVE_429_THRESHOLD
    halt_429_seconds: float = config.CIRCUIT_BREAKER_429_HALT_SECONDS
    halt_403_seconds: float = config.CIRCUIT_BREAKER_403_HALT_SECONDS
    halt_ban_seconds: float = config.CIRCUIT_BREAKER_BAN_HALT_SECONDS

    _consecutive_429: int = field(default=0, init=False)
    _halt_until: float | None = field(default=None, init=False)
    _manual_restart_required: bool = field(default=False, init=False)

    def check(self) -> None:
        if self._manual_restart_required:
            raise ManualRestartRequiredError(
                "Circuit breaker tripped on a ban-shaped response. Halted for "
                f"{self.halt_ban_seconds}s and requires a manual restart of this process "
                "even after the halt window elapses. Investigate before restarting."
            )
        if self._halt_until is not None:
            now = time.monotonic()
            if now < self._halt_until:
                raise CircuitOpenError(
                    f"Circuit breaker open. Halted until t+{self._halt_until - now:.0f}s from now."
                )
            logger.warning("circuit breaker halt window elapsed, resuming polling")
            self._halt_until = None
            self._consecutive_429 = 0

    def record_success(self) -> None:
        self._consecutive_429 = 0

    def record_429(self) -> None:
        self._consecutive_429 += 1
        logger.warning("429 received (consecutive=%d)", self._consecutive_429)
        if self._consecutive_429 >= self.consecutive_429_threshold:
            logger.error(
                "*** CIRCUIT BREAKER TRIPPED: %d consecutive 429s. Halting all polling for %.0fs. ***",
                self._consecutive_429,
                self.halt_429_seconds,
            )
            self._halt_until = time.monotonic() + self.halt_429_seconds

    def record_403(self) -> None:
        logger.error(
            "*** CIRCUIT BREAKER TRIPPED: 403 received. Halting all polling for %.0fs. ***",
            self.halt_403_seconds,
        )
        self._halt_until = time.monotonic() + self.halt_403_seconds

    def record_ban_shaped_response(self) -> None:
        logger.critical(
            "*** CIRCUIT BREAKER TRIPPED: ban-shaped response (HTML instead of JSON). "
            "Halting for %.0fs AND requiring a manual restart. ***",
            self.halt_ban_seconds,
        )
        self._halt_until = time.monotonic() + self.halt_ban_seconds
        self._manual_restart_required = True


def jittered_backoff_delay(attempt: int) -> float:
    """Exponential backoff with jitter for retrying after a 429. Never used to retry tightly."""
    base: float = min(
        config.RETRY_BASE_DELAY_SECONDS * (2 ** attempt),
        config.RETRY_MAX_DELAY_SECONDS,
    )
    return float(base + random.uniform(0, config.RETRY_JITTER_SECONDS))
