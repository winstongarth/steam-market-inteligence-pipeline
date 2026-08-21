"""Tests for detect_changes' stateful diffing logic, isolated from Spark entirely via a
mock GroupState (only .exists / .get / .update are used by the function — see
streaming/cdc_job.py). This is deliberately NOT a Spark-integration test: real
applyInPandasWithState execution needs a live streaming query with checkpointing, which
only works inside the Docker container (see conftest.py / cdc_job.py docstrings) — this
test instead proves the diffing logic itself is correct, fast, and dependency-free.

State tuples are (lowest_sell, highest_buy, sell_listings, volume, last_observed_at,
last_money_domain) — six elements, matching STATE_SCHEMA. `last_money_domain` is a string
like "currency:1" or "endpoint:search_render" — see cdc_job.py's `_money_domain`.
"""

import datetime

import pandas as pd
import pytest

from streaming.cdc_job import WATCHED_FIELDS, _money_domain, detect_changes


class FakeGroupState:
    def __init__(self, initial: tuple | None = None):
        self._value = initial
        self.exists = initial is not None
        self.updated_with: tuple | None = None

    @property
    def get(self):
        return self._value

    def update(self, value: tuple) -> None:
        self.updated_with = value
        self._value = value
        self.exists = True


def _pdf(rows: list[dict]) -> pd.DataFrame:
    columns = [
        "app_id", "market_hash_name", "observed_at",
        "lowest_sell", "highest_buy", "sell_listings", "volume", "currency", "source_endpoint",
    ]
    return pd.DataFrame(rows, columns=columns)


def _row(**overrides):
    base = {
        "app_id": 730, "market_hash_name": "Foo", "observed_at": datetime.datetime(2026, 1, 1, 0, 5),
        "lowest_sell": None, "highest_buy": None, "sell_listings": None, "volume": None,
        "currency": None, "source_endpoint": "priceoverview",
    }
    base.update(overrides)
    return base


def test_money_domain_uses_currency_when_present():
    assert _money_domain(1, "priceoverview") == "currency:1"
    assert _money_domain(8, "orderbook") == "currency:8"


def test_money_domain_falls_back_to_endpoint_when_currency_is_none():
    assert _money_domain(None, "search_render") == "endpoint:search_render"


def test_first_observation_seeds_state_without_emitting_a_change():
    state = FakeGroupState(initial=None)
    row = _row(observed_at=datetime.datetime(2026, 1, 1), lowest_sell=100, sell_listings=5, currency=1)
    result = list(detect_changes((730, "Foo"), iter([_pdf([row])]), state))
    events = pd.concat(result, ignore_index=True)

    assert events.empty
    assert state.exists
    assert state.updated_with == (100, None, 5, None, datetime.datetime(2026, 1, 1), "currency:1")


def test_field_change_emits_one_event_with_correct_delta_and_pct():
    prior_state = (100, None, 5, None, datetime.datetime(2026, 1, 1), "currency:1")
    state = FakeGroupState(initial=prior_state)
    row = _row(lowest_sell=110, sell_listings=5, currency=1)
    result = list(detect_changes((730, "Foo"), iter([_pdf([row])]), state))
    events = pd.concat(result, ignore_index=True)

    assert len(events) == 1
    event = events.iloc[0]
    assert event["field"] == "lowest_sell"
    assert event["previous"] == 100.0
    assert event["current"] == 110.0
    assert event["delta"] == 10.0
    assert event["pct_change"] == pytest.approx(10.0)


def test_unchanged_field_emits_nothing():
    prior_state = (100, None, 5, None, datetime.datetime(2026, 1, 1), "currency:1")
    state = FakeGroupState(initial=prior_state)
    row = _row(lowest_sell=100, sell_listings=5, currency=1)
    result = list(detect_changes((730, "Foo"), iter([_pdf([row])]), state))
    events = pd.concat(result, ignore_index=True)
    assert events.empty


def test_missing_field_in_this_endpoints_row_is_skipped_not_treated_as_change():
    """orderbook rows carry highest_buy; a later same-domain row without it must not
    clobber the previously-known highest_buy with a null."""
    prior_state = (100, 90, 5, None, datetime.datetime(2026, 1, 1), "currency:8")
    state = FakeGroupState(initial=prior_state)
    row = _row(lowest_sell=100, sell_listings=5, currency=8, source_endpoint="orderbook")
    result = list(detect_changes((730, "Foo"), iter([_pdf([row])]), state))
    events = pd.concat(result, ignore_index=True)
    assert events.empty
    assert state.updated_with[1] == 90  # highest_buy preserved, not clobbered by the null


def test_multiple_fields_changing_emits_multiple_events():
    prior_state = (100, 90, 5, None, datetime.datetime(2026, 1, 1), "currency:8")
    state = FakeGroupState(initial=prior_state)
    row = _row(lowest_sell=110, highest_buy=95, sell_listings=5, currency=8, source_endpoint="orderbook")
    result = list(detect_changes((730, "Foo"), iter([_pdf([row])]), state))
    events = pd.concat(result, ignore_index=True)
    assert set(events["field"]) == {"lowest_sell", "highest_buy"}
    assert len(events) == 2


def test_zero_previous_value_gives_null_pct_change_not_a_crash():
    prior_state = (0, None, None, None, datetime.datetime(2026, 1, 1), "currency:1")
    state = FakeGroupState(initial=prior_state)
    row = _row(lowest_sell=50, currency=1)
    result = list(detect_changes((730, "Foo"), iter([_pdf([row])]), state))
    events = pd.concat(result, ignore_index=True)
    assert len(events) == 1
    assert pd.isna(events.iloc[0]["pct_change"])


def test_watched_fields_covers_the_four_named_in_the_spec():
    assert set(WATCHED_FIELDS) == {"lowest_sell", "highest_buy", "sell_listings", "volume"}


# --- Money-domain-conflict regression tests (bug found and fixed TWICE, 2026-08-21) -----
#
# Bug #1 (fixed but insufficient): naively diffing lowest_sell across endpoints produced
# "lowest_sell moved from 3925 to 621400" (priceoverview USD-cents -> orderbook JPY minor
# units for the same item) — a 15,764%-looking "price move" that was actually a unit
# mismatch. First fix compared currency codes directly.
#
# Bug #2 (why bug #1's fix wasn't enough): that fix treated a null currency (search_render
# never reports one) as compatible with ANY real currency. An item whose baseline was
# built entirely from search_render observations kept a null tracked currency, so a later
# orderbook observation (currency=8) never tripped the "conflict" check — producing
# ANOTHER bogus event (3878 -> 620600, ~15,903%). Real fix: track a domain string
# (`_money_domain`), where null-currency rows get a domain tied to their specific source
# endpoint instead of a wildcard — see cdc_job.py's detect_changes docstring for the full
# account.


def test_currency_to_currency_conflict_reseeds_instead_of_emitting_a_bogus_event():
    prior_state = (3925, None, None, None, datetime.datetime(2026, 1, 1), "currency:1")  # USD baseline
    state = FakeGroupState(initial=prior_state)
    row = _row(lowest_sell=621400, highest_buy=621000, sell_listings=10, currency=8, source_endpoint="orderbook")

    result = list(detect_changes((730, "Foo"), iter([_pdf([row])]), state))
    events = pd.concat(result, ignore_index=True)

    assert events.empty  # no bogus "15,764% price move" event
    assert state.updated_with[0] == 621400  # lowest_sell reseeded to the new (JPY) value
    assert state.updated_with[5] == "currency:8"  # domain baseline updated


def test_null_currency_baseline_vs_real_currency_row_is_a_conflict():
    """This is exactly the second bug: a search_render-only baseline (domain
    "endpoint:search_render") must NOT be treated as compatible with an incoming
    orderbook row just because the baseline's currency happened to be null."""
    prior_state = (3878, None, None, None, datetime.datetime(2026, 1, 1), "endpoint:search_render")
    state = FakeGroupState(initial=prior_state)
    row = _row(lowest_sell=620600, currency=8, source_endpoint="orderbook")

    result = list(detect_changes((730, "Foo"), iter([_pdf([row])]), state))
    events = pd.concat(result, ignore_index=True)

    assert events.empty  # no bogus "15,903% price move" event
    assert state.updated_with[0] == 620600
    assert state.updated_with[5] == "currency:8"


def test_conflict_only_affects_fields_present_in_the_conflicting_row():
    prior_state = (3925, 3900, 10, None, datetime.datetime(2026, 1, 1), "currency:1")
    state = FakeGroupState(initial=prior_state)
    row = _row(lowest_sell=621400, currency=8, source_endpoint="orderbook")  # only lowest_sell present

    result = list(detect_changes((730, "Foo"), iter([_pdf([row])]), state))
    events = pd.concat(result, ignore_index=True)

    assert events.empty
    assert state.updated_with[0] == 621400  # reseeded
    assert state.updated_with[1] == 3900  # untouched fields preserved, not wiped
    assert state.updated_with[2] == 10


def test_two_search_render_observations_share_a_domain_and_diff_normally():
    """The whole point of endpoint-scoped domains: same-endpoint repeats must still
    compress normally, not get treated as a permanent conflict."""
    prior_state = (100, None, 5, None, datetime.datetime(2026, 1, 1), "endpoint:search_render")
    state = FakeGroupState(initial=prior_state)
    row = _row(lowest_sell=110, sell_listings=5, source_endpoint="search_render")  # currency=None

    result = list(detect_changes((730, "Foo"), iter([_pdf([row])]), state))
    events = pd.concat(result, ignore_index=True)

    assert len(events) == 1
    assert events.iloc[0]["field"] == "lowest_sell"
    assert state.updated_with[5] == "endpoint:search_render"


def test_same_currency_across_observations_diffs_normally():
    prior_state = (26000, 26100, None, None, datetime.datetime(2026, 1, 1), "currency:8")
    state = FakeGroupState(initial=prior_state)
    row = _row(lowest_sell=26200, highest_buy=26100, currency=8, source_endpoint="orderbook")

    result = list(detect_changes((730, "Foo"), iter([_pdf([row])]), state))
    events = pd.concat(result, ignore_index=True)

    assert len(events) == 1  # only lowest_sell actually changed; highest_buy stayed 26100
    assert events.iloc[0]["field"] == "lowest_sell"
