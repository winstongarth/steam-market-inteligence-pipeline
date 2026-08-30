"""
Measure the real rate limit. Throwaway script.

Ramps request rate against the cheapest endpoint (priceoverview, single item)
in discrete steps until the first 429, then STOPS IMMEDIATELY and measures
recovery time by polling with backoff, capped so this run stays bounded.

Rules this respects: we do not hammer through a 429, we do not retry
tightly, and we log every 429 as a first-class event.

Run: uv run python scripts/ratelimit.py
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import httpx

BASE = "https://steamcommunity.com"
USER_AGENT = (
    "steam-market-pipeline-recon/0.1 "
    "(personal research project; contact: winstonpatrickgarth@gmail.com)"
)
OUT = Path(__file__).resolve().parent.parent / "docs" / "evidence" / "rate-limit-probe.json"

APP_ID = 730
HASH_NAME = "AK-47 | Redline (Field-Tested)"

# Intervals to ramp through, seconds between requests. Requests-per-level is
# deliberately small so we stop close to the actual threshold instead of
# blowing far past it.
LEVELS = [2.0, 1.0, 0.5, 0.25, 0.1, 0.05, 0.0]
REQUESTS_PER_LEVEL = 8

MAX_RECOVERY_PROBE_SECONDS = 240  # cap so this run stays bounded


def main() -> None:
    client = httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=15.0)
    params = {"appid": APP_ID, "currency": 1, "market_hash_name": HASH_NAME}

    log: list[dict] = []
    total_requests = 0
    hit_429_at: dict | None = None
    t_start = time.monotonic()

    for interval in LEVELS:
        print(f"\n--- level: {interval}s between requests ---")
        for i in range(REQUESTS_PER_LEVEL):
            t0 = time.monotonic()
            r = client.get(f"{BASE}/market/priceoverview/", params=params)
            elapsed = time.monotonic() - t0
            total_requests += 1
            entry = {
                "n": total_requests,
                "interval_s": interval,
                "status": r.status_code,
                "request_elapsed_s": round(elapsed, 3),
                "t_since_start_s": round(time.monotonic() - t_start, 2),
            }
            log.append(entry)
            print(f"  req#{total_requests:>3} interval={interval}s status={r.status_code}")

            if r.status_code == 429:
                hit_429_at = entry
                hit_429_at["retry_after_header"] = r.headers.get("retry-after")
                hit_429_at["headers_sample"] = {
                    k: v for k, v in r.headers.items()
                    if k.lower() in ("retry-after", "x-ratelimit-limit", "x-ratelimit-remaining")
                }
                print(f"  *** 429 hit at request #{total_requests}, level {interval}s ***")
                break
            time.sleep(interval)
        if hit_429_at:
            break

    result = {
        "requests_before_429": total_requests if hit_429_at else None,
        "level_at_429_s": hit_429_at["interval_s"] if hit_429_at else None,
        "hit_429": hit_429_at,
        "full_log": log,
    }

    if not hit_429_at:
        print("\nNo 429 encountered across all ramp levels. Recording as inconclusive.")
        result["note"] = "No 429 observed up to the fastest tested interval (0s, effectively back-to-back)."
        OUT.parent.mkdir(parents=True, exist_ok=True)
        OUT.write_text(json.dumps(result, indent=2), encoding="utf-8")
        client.close()
        return

    # --- Recovery measurement: back off and probe, capped ---
    print("\nBacking off. Measuring recovery with capped exponential probes...")
    time.sleep(5.0)  # immediate cooldown before first probe, not a tight retry
    backoff = 10.0
    waited = 5.0
    recovered_after_s = None
    probes = []
    while waited < MAX_RECOVERY_PROBE_SECONDS:
        r = client.get(f"{BASE}/market/priceoverview/", params=params)
        probes.append({"waited_s": round(waited, 1), "status": r.status_code})
        print(f"  probe at t+{waited:.1f}s -> status {r.status_code}")
        if r.status_code == 200:
            recovered_after_s = waited
            break
        time.sleep(backoff)
        waited += backoff
        backoff = min(backoff * 1.5, 60.0)

    result["recovery_probes"] = probes
    result["recovered_after_s"] = recovered_after_s
    if recovered_after_s is None:
        result["recovery_note"] = (
            f"Not recovered within the {MAX_RECOVERY_PROBE_SECONDS}s capped probe window. "
            "Actual recovery time is longer than measured here; needs a longer standalone check."
        )

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(f"\nWrote {OUT}")
    client.close()


if __name__ == "__main__":
    main()
