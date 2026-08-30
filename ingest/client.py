"""Async HTTP client for the Steam Community Market: rate limiting, retries, structured logging.

Every request goes through the shared RateLimitedClient so the token bucket and circuit
breaker (ratelimit.py) are enforced uniformly regardless of which endpoint module is calling.
"""

from __future__ import annotations

import logging

from typing import Any

import httpx

from ingest import config
from ingest.ratelimit import CircuitBreaker, ManualRestartRequiredError, TokenBucket, jittered_backoff_delay

logger = logging.getLogger("ingest.client")

# A ban-shaped response: Steam serving an HTML error/challenge page where JSON was expected.
_BAN_SHAPED_MARKERS = (b"<html", b"<!DOCTYPE")


class SteamMarketClient:
    """Wraps httpx.AsyncClient with the project's rate limiting and circuit breaker."""

    def __init__(
        self,
        bucket: TokenBucket | None = None,
        breaker: CircuitBreaker | None = None,
        max_retries: int = 3,
    ) -> None:
        self.bucket = bucket or TokenBucket(rate_per_second=config.GLOBAL_REQUESTS_PER_SECOND)
        self.breaker = breaker or CircuitBreaker()
        self.max_retries = max_retries
        self._client = httpx.AsyncClient(
            headers={"User-Agent": config.USER_AGENT},
            timeout=config.REQUEST_TIMEOUT_SECONDS,
            follow_redirects=True,  # the listings page 302s into the bucket SPA page
        )

    async def __aenter__(self) -> "SteamMarketClient":
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()

    async def close(self) -> None:
        await self._client.aclose()

    async def get_json(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        expect_json: bool = True,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, object]:
        """GET a URL, returning (status_code, parsed_body_or_raw_text).

        Handles the full rate-limit / retry / circuit-breaker lifecycle. Raises
        CircuitOpenError or ManualRestartRequiredError if the breaker is tripped —
        callers (the scheduler) should let those propagate and stop polling.
        """
        attempt = 0
        while True:
            self.breaker.check()  # raises if halted
            await self.bucket.acquire()

            logger.info("GET %s params=%s attempt=%d", url, params, attempt)
            response = await self._client.get(url, params=params, headers=headers)
            status = response.status_code

            # Ban-shape detection means "HTML where JSON was expected" — endpoints fetched with
            # expect_json=False (e.g. the listings/bucket page) are legitimately HTML.
            if expect_json and _looks_ban_shaped(response):
                self.breaker.record_ban_shaped_response()
                raise ManualRestartRequiredError("ban-shaped response received")

            if status == 429:
                self.breaker.record_429()
                self.bucket.reduce_for_rest_of_hour(config.BACKOFF_REFILL_REDUCTION_FACTOR)
                if attempt >= self.max_retries:
                    logger.error("giving up after %d retries on 429: %s", attempt, url)
                    return status, response.text
                delay = jittered_backoff_delay(attempt)
                logger.warning("429 on %s — backing off %.1fs (attempt %d)", url, delay, attempt)
                attempt += 1
                await _sleep(delay)
                continue

            if status == 403:
                self.breaker.record_403()
                return status, response.text

            self.breaker.record_success()

            if not expect_json:
                return status, response.text

            try:
                return status, response.json()
            except ValueError:
                logger.warning("non-JSON body from %s (status %d), returning raw text", url, status)
                return status, response.text


async def _sleep(seconds: float) -> None:
    import asyncio

    await asyncio.sleep(seconds)


def _looks_ban_shaped(response: httpx.Response) -> bool:
    content_type = response.headers.get("content-type", "")
    if "json" in content_type:
        return False
    body_start = response.content[:200].lstrip()
    return any(body_start.startswith(marker) for marker in _BAN_SHAPED_MARKERS)
