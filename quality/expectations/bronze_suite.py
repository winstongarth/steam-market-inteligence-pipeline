"""Great Expectations suite for raw Bronze data (schema drift + freshness).

Runs BEFORE any dbt transform, per the DAG's step order (run_quality_gates ->
dbt_run_staging -> ...) — these are checks on the raw envelope shape and recency, not on
modeled data. The remaining Phase 4 checks (price plausibility, crossed book, volume
monotonicity, referential integrity, uniqueness) are dbt tests instead
(dbt/tests/assert_*.sql) — they're about modeled/transformed data, where dbt's own test
framework is the more natural fit than re-deriving the same logic in GX. See
docs/DECISIONS.md for that split rationale.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import great_expectations as gx
import pandas as pd

logger = logging.getLogger("quality.bronze_suite")

# The full known shape of a RawEnvelope (ingest/schemas.py) — schema drift means one of
# these goes missing.
EXPECTED_ENVELOPE_COLUMNS = [
    "source", "endpoint", "app_id", "currency", "observed_at",
    "request_params", "raw_payload", "ingest_id",
]

DEFAULT_FRESHNESS_SLA_HOURS = 25.0  # Tier C polls daily; +1h buffer for scheduling jitter


@dataclass
class GateResult:
    success: bool
    summary: str
    failed_expectations: list[str] = field(default_factory=list)


def run_bronze_quality_gates(
    envelopes_df: pd.DataFrame,
    freshness_sla_hours: float = DEFAULT_FRESHNESS_SLA_HOURS,
    now: datetime | None = None,
) -> GateResult:
    """Validates a Bronze envelope DataFrame (columns matching RawEnvelope) against schema
    drift and freshness expectations. Raises nothing — returns a GateResult; the caller
    (the Airflow task) decides whether a failure should block the DAG."""
    now = now or datetime.now(timezone.utc)
    freshness_floor = now - timedelta(hours=freshness_sla_hours)

    context = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_pandas("bronze_pandas_source")
    asset = data_source.add_dataframe_asset(name="bronze_envelopes")
    batch_def = asset.add_batch_definition_whole_dataframe("bronze_batch")
    batch = batch_def.get_batch(batch_parameters={"dataframe": envelopes_df})

    suite = context.suites.add(gx.ExpectationSuite(name="bronze_suite"))

    # Schema drift: every expected field must still exist.
    for column in EXPECTED_ENVELOPE_COLUMNS:
        suite.add_expectation(gx.expectations.ExpectColumnToExist(column=column))

    # Structural not-null — these fields should never be missing on a well-formed envelope.
    for column in ["source", "endpoint", "app_id", "observed_at", "raw_payload", "ingest_id"]:
        if column in envelopes_df.columns:
            suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column=column))

    # Freshness: the newest observation must be within the SLA window.
    if "observed_at" in envelopes_df.columns:
        suite.add_expectation(
            gx.expectations.ExpectColumnMaxToBeBetween(
                column="observed_at",
                min_value=freshness_floor,
                max_value=now,
            )
        )

    result = batch.validate(suite)

    failed = [r.expectation_config.type for r in result.results if not r.success]
    summary = (
        f"{len(result.results) - len(failed)}/{len(result.results)} expectations passed"
        f" (freshness SLA={freshness_sla_hours}h)"
    )
    if failed:
        logger.error("Bronze quality gate FAILED: %s. Failed expectations: %s", summary, failed)
    else:
        logger.info("Bronze quality gate passed: %s", summary)

    return GateResult(success=result.success, summary=summary, failed_expectations=failed)
