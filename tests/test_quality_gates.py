"""Tests for quality/expectations/bronze_suite.py — the raw-layer GX quality gate.
No network, no Docker — builds small synthetic DataFrames matching RawEnvelope's shape.
"""

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from quality.expectations.bronze_suite import EXPECTED_ENVELOPE_COLUMNS, run_bronze_quality_gates


def _good_envelopes_df(n: int = 3) -> pd.DataFrame:
    now = datetime.now(timezone.utc)
    return pd.DataFrame(
        {
            "source": ["steamcommunity"] * n,
            "endpoint": ["search_render"] * n,
            "app_id": [730] * n,
            "currency": [None] * n,
            "observed_at": [now - timedelta(minutes=i) for i in range(n)],
            "request_params": ["{}"] * n,
            "raw_payload": ["{}"] * n,
            "ingest_id": [f"id-{i}" for i in range(n)],
        }
    )


def test_gate_passes_on_well_formed_data():
    result = run_bronze_quality_gates(_good_envelopes_df())
    assert result.success
    assert result.failed_expectations == []


def test_gate_fails_on_missing_column_schema_drift():
    df = _good_envelopes_df().drop(columns=["endpoint"])
    result = run_bronze_quality_gates(df)
    assert not result.success
    assert "expect_column_to_exist" in result.failed_expectations


def test_gate_fails_on_stale_data():
    df = _good_envelopes_df()
    df["observed_at"] = df["observed_at"] - timedelta(hours=48)
    result = run_bronze_quality_gates(df, freshness_sla_hours=25.0)
    assert not result.success
    assert "expect_column_max_to_be_between" in result.failed_expectations


def test_gate_fails_on_null_app_id():
    df = _good_envelopes_df()
    df.loc[0, "app_id"] = None
    result = run_bronze_quality_gates(df)
    assert not result.success
    assert "expect_column_values_to_not_be_null" in result.failed_expectations


def test_expected_columns_match_raw_envelope_schema():
    """Guards against this list silently drifting from ingest/schemas.py's RawEnvelope."""
    from ingest.schemas import RawEnvelope

    assert set(EXPECTED_ENVELOPE_COLUMNS) == set(RawEnvelope.model_fields.keys())
