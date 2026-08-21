"""Money parsing tests — pure functions, no Spark/network needed. Verified against the
real currency-format samples recorded in tests/fixtures/currency_probe_extended.json
(Phase 0)."""

import json
from pathlib import Path

import pytest

from streaming.money import parse_price_string, parse_volume_string

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.mark.parametrize(
    "text,currency_code,expected_minor_units",
    [
        ("$39.25", 1, 3925),
        ("£28.84", 2, 2884),
        ("33,72€", 3, 3372),
        ("CHF 31.65", 4, 3165),
        ("3338,26 руб.", 5, 333826),
        ("R$ 202,91", 7, 20291),
        ("¥ 6,228", 8, 622800),
        ("Rp 700 697", 10, 70069700),
        ("S$49.99", 13, 4999),
        ("1 757₴", 18, 175700),
        ("CDN$ 54.37", 20, 5437),
        ("¥ 266.58", 23, 26658),
        ("₹ 3,756", 24, 375600),
        ("₹ 5,023.43", 24, 502343),
    ],
)
def test_parse_price_string_real_observed_formats(text, currency_code, expected_minor_units):
    assert parse_price_string(text, currency_code) == expected_minor_units


def test_parse_price_string_unrecognized_currency_returns_none():
    assert parse_price_string("¥ 6,228", 999) is None


def test_parse_price_string_none_input():
    assert parse_price_string(None, 1) is None
    assert parse_price_string("$39.25", None) is None


def test_parse_volume_string():
    assert parse_volume_string("105") == 105
    assert parse_volume_string(None) is None


def test_against_real_fixture_median_and_lowest_prices():
    """Cross-check against the actual recorded Phase 0 fixture, not just hand-copied values."""
    data = json.loads((FIXTURES / "currency_probe_extended.json").read_text(encoding="utf-8"))
    for code_str, entry in data.items():
        code = int(code_str)
        body = entry["body"]
        lowest = parse_price_string(body["lowest_price"], code)
        median = parse_price_string(body["median_price"], code)
        assert lowest is not None, f"failed to parse lowest_price for currency {code}: {body['lowest_price']!r}"
        assert median is not None, f"failed to parse median_price for currency {code}: {body['median_price']!r}"
        assert lowest <= median, f"currency {code}: lowest ({lowest}) should not exceed median ({median})"
