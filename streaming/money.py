"""Money-string parsing. All money is parsed to integer minor units plus a currency
code here, once.

Deliberately narrow: only recognizes the currency display formats actually observed and
verified (docs/FINDINGS.md). An unrecognized currency code returns None rather than a
guessed value — never silently misparse a price.

Currency code comes from the request that produced the string (RawEnvelope.currency, or
the order-book endpoint's own eCurrency field) — not reverse-inferred from the symbol.
That sidesteps a real ambiguity: JPY (8) and CNY (23) both display with the same "¥"
symbol, so the symbol alone can't tell them apart, but the code always can.

Values are always returned as integer minor units at an implied 2 decimal places
(amount * 100), matching the convention Steam's own /market/orderbook endpoint uses even
for zero-decimal currencies like JPY — see docs/DECISIONS.md for that finding. This keeps
one consistent representation across every endpoint's money fields, rather than varying
scale by each currency's "real" decimal count.
"""

from __future__ import annotations

import re

# thousands / decimal separator characters, per currency code, derived from real observed
# samples (docs/FINDINGS.md). decimal=None means the sample never showed a
# fractional part (Steam appears to omit ".00"-equivalent suffixes for whole amounts —
# verified this doesn't break parsing, since the decimal separator is optional whenever it
# appears in the input, not assumed present).
CURRENCY_NUMBER_FORMATS: dict[int, dict[str, str | None]] = {
    1: {"thousands": ",", "decimal": "."},   # USD  "$39.25"
    2: {"thousands": ",", "decimal": "."},   # GBP  "£28.84"
    3: {"thousands": ".", "decimal": ","},   # EUR  "33,72€"
    4: {"thousands": ",", "decimal": "."},   # CHF  "CHF 31.65"
    5: {"thousands": ".", "decimal": ","},   # RUB  "3338,26 руб."
    7: {"thousands": ".", "decimal": ","},   # BRL  "R$ 202,91"
    8: {"thousands": ",", "decimal": None},  # JPY  "¥ 6,228"
    10: {"thousands": " ", "decimal": None},  # IDR  "Rp 700 697"
    13: {"thousands": ",", "decimal": "."},   # SGD  "S$49.99"
    18: {"thousands": " ", "decimal": None},  # UAH  "1 757₴"
    20: {"thousands": ",", "decimal": "."},   # CAD  "CDN$ 54.37"
    23: {"thousands": ",", "decimal": "."},   # CNY  "¥ 266.58"
    24: {"thousands": ",", "decimal": "."},   # INR  "₹ 3,756" / "₹ 5,023.43"
}


def parse_price_string(text: str | None, currency_code: int | None) -> int | None:
    """Returns integer minor units (amount * 100), or None if unparseable/unrecognized."""
    if not text or currency_code is None:
        return None
    fmt = CURRENCY_NUMBER_FORMATS.get(currency_code)
    if fmt is None:
        return None

    thousands_sep = fmt["thousands"]
    decimal_sep = fmt["decimal"]

    allowed = "0-9" + re.escape(thousands_sep or "") + re.escape(decimal_sep or "")
    kept = "".join(re.findall(f"[{allowed}]", text))
    if thousands_sep:
        kept = kept.replace(thousands_sep, "")
    if decimal_sep:
        kept = kept.replace(decimal_sep, ".")

    if not kept:
        return None
    try:
        value = float(kept)
    except ValueError:
        return None
    return round(value * 100)


def parse_volume_string(text: str | None) -> int | None:
    """priceoverview's `volume` field is a plain integer-as-string trade count, not a
    currency amount — parsed separately from price fields, never through
    parse_price_string (which would wrongly scale it by 100)."""
    if not text:
        return None
    digits = "".join(ch for ch in text if ch.isdigit())
    return int(digits) if digits else None
