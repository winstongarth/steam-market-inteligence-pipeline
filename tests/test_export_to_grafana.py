"""Tests for analytics/export_to_grafana.py's pure transform logic, using synthetic
DataFrames — no DuckDB/Postgres dependency, matching this project's convention of never
hitting live services in tests.
"""

from datetime import datetime, timezone

import pandas as pd
import pytest

from analytics.export_to_grafana import (
    filter_by_app_ids,
    parse_app_ids,
    read_marts,
    stamp_synced_at,
)


def test_parse_app_ids_defaults_to_none_on_empty():
    assert parse_app_ids(None) is None
    assert parse_app_ids("") is None
    assert parse_app_ids("   ") is None


def test_parse_app_ids_splits_and_casts_to_int():
    assert parse_app_ids("730") == [730]
    assert parse_app_ids("730, 570 ,440") == [730, 570, 440]


def test_filter_by_app_ids_restricts_to_given_games():
    df = pd.DataFrame({"app_id": [730, 730, 570], "market_hash_name": ["a", "b", "c"]})
    out = filter_by_app_ids(df, [730])
    assert out["app_id"].tolist() == [730, 730]
    assert out["market_hash_name"].tolist() == ["a", "b"]


def test_filter_by_app_ids_is_noop_when_app_ids_is_none():
    df = pd.DataFrame({"app_id": [730, 570]})
    out = filter_by_app_ids(df, None)
    assert out["app_id"].tolist() == [730, 570]


def test_filter_by_app_ids_is_noop_on_empty_df():
    df = pd.DataFrame(columns=["app_id", "market_hash_name"])
    out = filter_by_app_ids(df, [730])
    assert out.empty


def test_filter_by_app_ids_is_noop_when_column_missing():
    # Defensive path: read_marts calls this generically over every mart table without a
    # per-table special case, so a table without an app_id column must pass through.
    df = pd.DataFrame({"iso_code": ["USD", "EUR"]})
    out = filter_by_app_ids(df, [730])
    assert out["iso_code"].tolist() == ["USD", "EUR"]


def test_stamp_synced_at_adds_column_without_mutating_input():
    df = pd.DataFrame({"app_id": [730]})
    synced_at = datetime(2026, 8, 26, tzinfo=timezone.utc)
    out = stamp_synced_at(df, synced_at)
    assert "synced_at" not in df.columns
    assert (out["synced_at"] == synced_at).all()


class _FakeConnection:
    """Stands in for a duckdb.DuckDBPyConnection: `.execute(sql).fetchdf()`."""

    def __init__(self, tables: dict[str, pd.DataFrame]):
        self._tables = tables

    def execute(self, sql: str) -> "_FakeConnection":
        table_name = sql.strip().split("main.")[-1]
        if table_name not in self._tables:
            raise Exception(f"Catalog Error: Table with name {table_name} does not exist")
        self._pending = self._tables[table_name]
        return self

    def fetchdf(self) -> pd.DataFrame:
        return self._pending


def test_read_marts_filters_each_table_by_app_ids():
    con = _FakeConnection({
        "dim_item": pd.DataFrame({"app_id": [730, 570], "market_hash_name": ["x", "y"]}),
        "mart_item_daily": pd.DataFrame({"app_id": [730, 570], "close_price": [1.0, 2.0]}),
    })
    result = read_marts(con, [730], tables=["dim_item", "mart_item_daily"])
    assert result["dim_item"]["app_id"].tolist() == [730]
    assert result["mart_item_daily"]["close_price"].tolist() == [1.0]


def test_read_marts_skips_a_table_that_does_not_exist_yet():
    # mart_anomaly may not exist before the first anomaly-detection run — the sync should
    # still succeed for the tables that do exist rather than failing the whole run.
    con = _FakeConnection({"dim_item": pd.DataFrame({"app_id": [730]})})
    result = read_marts(con, [730], tables=["dim_item", "mart_anomaly"])
    assert set(result.keys()) == {"dim_item"}
