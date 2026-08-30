"""
API recon — throwaway script.

Hits every endpoint in scope exactly once, pretty-prints the response, and saves it to
tests/fixtures/. Also fetches robots.txt for compliance review.

This is NOT production code. It exists to confirm reality (field names, types,
whether pricehistory needs a login cookie) before any pipeline code is written.

Run: uv run python scripts/recon.py
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from urllib.parse import quote

import httpx

BASE = "https://steamcommunity.com"
FIXTURES = Path(__file__).resolve().parent.parent / "tests" / "fixtures"
FINDINGS = Path(__file__).resolve().parent.parent / "docs" / "FINDINGS.md"

USER_AGENT = (
    "steam-market-pipeline-recon/0.1 "
    "(personal research project; contact: winstonpatrickgarth@gmail.com)"
)

# Conservative placeholder delay between recon requests.
# NOT the production rate — that is set only after the ramp test measures a real limit.
RECON_DELAY_SECONDS = 3.0

APP_ID = 730  # CS2
HASH_NAME = "AK-47 | Redline (Field-Tested)"

findings: list[str] = []


def log(line: str) -> None:
    print(line)
    findings.append(line)


def save_fixture(name: str, content: str) -> None:
    FIXTURES.mkdir(parents=True, exist_ok=True)
    path = FIXTURES / name
    path.write_text(content, encoding="utf-8")
    log(f"  saved -> {path.relative_to(FIXTURES.parent.parent)}")


def pretty_json(text: str) -> tuple[str, dict | list | None]:
    try:
        parsed = json.loads(text)
        return json.dumps(parsed, indent=2, ensure_ascii=False), parsed
    except json.JSONDecodeError:
        return text, None


def main() -> None:
    client = httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=15.0)

    log(f"# Recon findings — recorded {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    log(f"User-Agent used: `{USER_AGENT}`\n")

    # --- robots.txt -------------------------------------------------------
    log("## robots.txt\n")
    r = client.get(f"{BASE}/robots.txt")
    save_fixture("robots.txt", r.text)
    log(f"- status: {r.status_code}")
    log("```\n" + r.text.strip() + "\n```\n")
    time.sleep(RECON_DELAY_SECONDS)

    # --- 3.1 Breadth: search/render ---------------------------------------
    log("## 3.1 /market/search/render/\n")
    params = {
        "query": "",
        "start": 0,
        "count": 100,
        "search_descriptions": 0,
        "sort_column": "popular",
        "sort_dir": "desc",
        "appid": APP_ID,
        "norender": 1,
    }
    r = client.get(f"{BASE}/market/search/render/", params=params)
    pretty, parsed = pretty_json(r.text)
    save_fixture("search_render.json", pretty)
    log(f"- status: {r.status_code}")
    if isinstance(parsed, dict):
        log(f"- top-level keys: {sorted(parsed.keys())}")
        results = parsed.get("results")
        if isinstance(results, list) and results:
            log(f"- results returned: {len(results)}")
            log(f"- first result keys: {sorted(results[0].keys())}")
            log("- first result sample:")
            log("```json\n" + json.dumps(results[0], indent=2, ensure_ascii=False) + "\n```")
    log("")
    time.sleep(RECON_DELAY_SECONDS)

    # --- count > 100 check --------------------------------------------------
    log("## 3.1b count=150 rejection check\n")
    params_over = dict(params, count=150)
    r = client.get(f"{BASE}/market/search/render/", params=params_over)
    pretty, parsed = pretty_json(r.text)
    save_fixture("search_render_count150.json", pretty)
    n = len(parsed.get("results", [])) if isinstance(parsed, dict) else None
    log(f"- status: {r.status_code}, results returned for count=150 request: {n}")
    log("")
    time.sleep(RECON_DELAY_SECONDS)

    # --- 3.3 item_nameid resolution ---------------------------------------
    log(f"## 3.3 /market/listings/{APP_ID}/{{hash_name}} (item_nameid resolution)\n")
    log(f"- target hash_name: `{HASH_NAME}`")
    url = f"{BASE}/market/listings/{APP_ID}/{quote(HASH_NAME)}"
    r = client.get(url)
    save_fixture("listings_page.html", r.text)
    log(f"- status: {r.status_code}")

    import re

    m = re.search(r"Market_LoadOrderSpread\(\s*(\d+)\s*\)", r.text)
    item_nameid = m.group(1) if m else None
    log(f"- regex `Market_LoadOrderSpread\\(\\s*(\\d+)\\s*\\)` match: {item_nameid}")
    if item_nameid:
        cache_path = Path(__file__).resolve().parent.parent / "data" / "cache" / "item_nameids.json"
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache = {}
        if cache_path.exists():
            cache = json.loads(cache_path.read_text())
        cache[f"{APP_ID}:{HASH_NAME}"] = item_nameid
        cache_path.write_text(json.dumps(cache, indent=2), encoding="utf-8")
        log(f"  cached -> data/cache/item_nameids.json (gitignored, permanent local cache)")
    log("")
    time.sleep(RECON_DELAY_SECONDS)

    # --- 3.2 Depth: itemordershistogram ------------------------------------
    log("## 3.2 /market/itemordershistogram/\n")
    if item_nameid:
        params = {
            "country": "US",
            "language": "english",
            "currency": 1,
            "item_nameid": item_nameid,
        }
        r = client.get(f"{BASE}/market/itemordershistogram/", params=params)
        pretty, parsed = pretty_json(r.text)
        save_fixture("itemordershistogram.json", pretty)
        log(f"- status: {r.status_code}")
        if isinstance(parsed, dict):
            log(f"- top-level keys: {sorted(parsed.keys())}")
    else:
        log("- SKIPPED: no item_nameid resolved")
    log("")
    time.sleep(RECON_DELAY_SECONDS)

    # --- 3.4 Point price: priceoverview -------------------------------------
    log("## 3.4 /market/priceoverview/\n")
    params = {"appid": APP_ID, "currency": 1, "market_hash_name": HASH_NAME}
    r = client.get(f"{BASE}/market/priceoverview/", params=params)
    pretty, parsed = pretty_json(r.text)
    save_fixture("priceoverview.json", pretty)
    log(f"- status: {r.status_code}")
    if isinstance(parsed, dict):
        log(f"- keys: {sorted(parsed.keys())}")
        log("```json\n" + json.dumps(parsed, indent=2, ensure_ascii=False) + "\n```")
    log("")
    time.sleep(RECON_DELAY_SECONDS)

    # --- 3.5 Activity: itemordersactivity -----------------------------------
    log("## 3.5 /market/itemordersactivity/\n")
    if item_nameid:
        params = {
            "country": "US",
            "language": "english",
            "currency": 1,
            "item_nameid": item_nameid,
        }
        r = client.get(f"{BASE}/market/itemordersactivity/", params=params)
        pretty, parsed = pretty_json(r.text)
        save_fixture("itemordersactivity.json", pretty)
        log(f"- status: {r.status_code}")
        if isinstance(parsed, dict):
            log(f"- keys: {sorted(parsed.keys())}")
    else:
        log("- SKIPPED: no item_nameid resolved")
    log("")
    time.sleep(RECON_DELAY_SECONDS)

    # --- pricehistory without login -------------------------
    log("## /market/pricehistory/ without a login cookie\n")
    params = {"appid": APP_ID, "market_hash_name": HASH_NAME}
    r = client.get(f"{BASE}/market/pricehistory/", params=params)
    pretty, parsed = pretty_json(r.text)
    save_fixture("pricehistory_no_login.json", pretty)
    log(f"- status: {r.status_code}")
    if isinstance(parsed, dict):
        log(f"- keys: {sorted(parsed.keys())}")
        log(f"- success field: {parsed.get('success')!r}")
    log("```json\n" + pretty[:1000] + ("\n... (truncated)" if len(pretty) > 1000 else "") + "\n```")
    log("")

    # --- 3.7 currency codes: quick cross-check ------------------------------
    log("## 3.7 currency code spot-check\n")
    log("Requesting priceoverview for the same item across candidate currency codes.")
    log("Each request is 3s apart (recon pace, not the measured production rate).\n")
    currency_candidates = {1: "USD", 2: "GBP", 3: "EUR", 20: "SGD", 23: "IDR"}
    currency_results = {}
    for code, label in currency_candidates.items():
        time.sleep(RECON_DELAY_SECONDS)
        params = {"appid": APP_ID, "currency": code, "market_hash_name": HASH_NAME}
        r = client.get(f"{BASE}/market/priceoverview/", params=params)
        _, parsed = pretty_json(r.text)
        price_text = parsed.get("lowest_price") if isinstance(parsed, dict) else None
        currency_results[code] = {"assumed_label": label, "status": r.status_code, "lowest_price": price_text}
        log(f"- currency={code} (assumed {label}): status={r.status_code}, lowest_price={price_text!r}")
    save_fixture("currency_check.json", json.dumps(currency_results, indent=2, ensure_ascii=False))
    log("")

    client.close()

    FINDINGS.parent.mkdir(parents=True, exist_ok=True)
    FINDINGS.write_text("\n".join(findings) + "\n", encoding="utf-8")
    log(f"\nFindings written to {FINDINGS}")


if __name__ == "__main__":
    main()
