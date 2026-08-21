"""Token bucket and circuit breaker unit tests. No network involved."""

import time

import pytest

from ingest.ratelimit import CircuitBreaker, CircuitOpenError, ManualRestartRequiredError, TokenBucket


@pytest.mark.asyncio
async def test_token_bucket_throttles_to_configured_rate():
    bucket = TokenBucket(rate_per_second=10.0, capacity=1.0)
    start = time.monotonic()
    for _ in range(5):
        await bucket.acquire()
    elapsed = time.monotonic() - start
    # 5 tokens at 10/s with capacity 1 should take at least ~0.4s (4 waits of ~0.1s)
    assert elapsed >= 0.3


@pytest.mark.asyncio
async def test_token_bucket_reduce_for_rest_of_hour_lowers_effective_rate():
    bucket = TokenBucket(rate_per_second=10.0, capacity=1.0)
    bucket.reduce_for_rest_of_hour(0.1)
    assert bucket._current_rate == pytest.approx(1.0)


def test_circuit_breaker_opens_after_consecutive_429s():
    breaker = CircuitBreaker(consecutive_429_threshold=3, halt_429_seconds=60)
    breaker.check()  # closed initially
    breaker.record_429()
    breaker.record_429()
    breaker.check()  # still closed at 2
    breaker.record_429()
    with pytest.raises(CircuitOpenError):
        breaker.check()


def test_circuit_breaker_success_resets_consecutive_count():
    breaker = CircuitBreaker(consecutive_429_threshold=3, halt_429_seconds=60)
    breaker.record_429()
    breaker.record_429()
    breaker.record_success()
    breaker.record_429()
    breaker.check()  # only 1 consecutive since reset — should not trip


def test_circuit_breaker_ban_shaped_response_requires_manual_restart():
    breaker = CircuitBreaker(halt_ban_seconds=60)
    breaker.record_ban_shaped_response()
    with pytest.raises(ManualRestartRequiredError):
        breaker.check()
