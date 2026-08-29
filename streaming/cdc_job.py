"""Phase 2 — Streaming CDC and Silver normalization.

Reads `market.raw.v1` (every poll response, verbatim), normalizes each endpoint's
heterogeneous raw shape into one common per-item schema, holds last-known state per
(app_id, market_hash_name), and emits to `market.changes.v1` only when a watched field
actually moves: `{app_id, market_hash_name, field, previous, current, delta, pct_change,
observed_at}` — one row per (item, changed field), not one row per item-update, so
downstream consumers can filter by field cheaply.

## Run this inside the `spark` container, not on the Windows host

Structured Streaming's checkpointing needs Hadoop's native Windows IO layer
(`winutils.exe`/`hadoop.dll`), which this project does not ship and which broke on first
attempt (`UnsatisfiedLinkError` in `NativeIO$Windows`). Running inside the Linux
`spark` container (docker-compose.yml) sidesteps this entirely — see docs/DECISIONS.md.

    docker exec steam-spark /opt/spark/bin/spark-submit \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 \\
        --conf spark.jars.ivy=/tmp/.ivy2 \\
        /opt/app/streaming/cdc_job.py

## API substitution: flatMapGroupsWithState -> applyInPandasWithState

The typed `flatMapGroupsWithState` API is Scala/Java-only; PySpark has never exposed it.
`applyInPandasWithState` (Spark 3.4+) is the direct Python-facing equivalent: same
`GroupState` execution engine underneath,
same arbitrary-stateful-processing semantics, just a Pandas-UDF-shaped interface instead
of a typed case class. Used here as a deliberate, documented substitution — not a shortcut
around the requirement.

## Watermark: 10 minutes on `observed_at`

Tier A polls every 5 minutes (§7.1.5) — the fastest cadence in the system. A watermark
needs enough slack to tolerate ordinary scheduling jitter and cross-tier out-of-order
delivery without holding every window open indefinitely. 10 minutes is 2x the fastest
tier's interval; revisit this if Tier A's cadence changes.

## Silver normalization — per-endpoint field mapping

Raw envelopes are heterogeneous by *endpoint*, not just by *game*:

| unified field    | search_render                          | priceoverview                    | orderbook                              |
|-------------------|-----------------------------------------|-----------------------------------|------------------------------------------|
| market_hash_name  | exploded `result.hash_name` (one row per array element) | Kafka key, after `:` | Kafka key, after `:` |
| lowest_sell       | `result.sell_price` (already int cents) | `parse_price_string(lowest_price, request_currency)` | `data.data.amtMinSellOrder` |
| highest_buy       | null (not present in this endpoint)     | null (not present)                | `data.data.amtMaxBuyOrder` |
| sell_listings     | `result.sell_listings`                  | null                               | `data.data.cSellOrders` |
| volume            | null                                     | `parse_volume_string(volume)` — a plain trade-count string, parsed separately from price fields so it's never wrongly scaled by 100 | null (`cBuyOrders`/`cSellOrders` are book-depth counts, not trade volume — deliberately not conflated) |
| currency          | null (this endpoint doesn't take/return one) | `request_currency` (the envelope's own `currency` field, i.e. what we asked for — not reverse-inferred from the string, which is ambiguous for JPY vs CNY, see streaming/money.py) | `data.data.eCurrency` (server-inferred; not currently controllable — open question, docs/DECISIONS.md) |

**Games-level normalization caveat, stated plainly:** The unified schema is meant to
cover CS2/Dota 2/TF2/Rust/PUBG's item taxonomies. Phase 0/1 only ever fetched real data for
CS2 (`appid=730`) — the other four app_ids were never sampled. This schema only relies on
fields already confirmed present for CS2 (`hash_name`, `sell_price`, `sell_listings`), so
it *should* generalize, but that is an assumption, not a verified fact. Flagged as an open
Phase 2 follow-up, not silently assumed away.

## Spread / depth (Phase 2 step 5) — scoped out of this file's CDC diffing, on purpose

`streaming/depth.py` implements and tests `spread`, `spread_bps`, and
`depth_within_pct` (at any %, so 1/5/10% are just call-site choices) against real
`orderbook` data. They are NOT wired into this job's watched-field set — the CDC job
watches exactly four fields (`lowest_sell`, `highest_buy`, `sell_listings`, `volume`);
spread/depth are *derived* metrics handled as a separate step, and they only apply to
`orderbook`-sourced rows (the only endpoint with
full order-book arrays). Folding 8 more derived fields into the CDC diff set now would
blur that scope. They're ready to compute in Phase 3's Gold layer (`mart_item_daily`)
directly from Silver's `orderbook` rows, which is where they belong architecturally.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, explode, expr, from_json, lit, to_timestamp, udf
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from streaming.money import parse_price_string, parse_volume_string

logger = logging.getLogger("streaming.cdc_job")

KAFKA_RAW_TOPIC = "market.raw.v1"
KAFKA_CHANGES_TOPIC = "market.changes.v1"
WATERMARK = "10 minutes"

WATCHED_FIELDS = ["lowest_sell", "highest_buy", "sell_listings", "volume"]

UNIFIED_COLUMNS = [
    "app_id", "market_hash_name", "observed_at",
    "lowest_sell", "highest_buy", "sell_listings", "volume", "currency",
    "source_endpoint",
]

STATE_SCHEMA = StructType(
    [StructField(f, LongType(), nullable=True) for f in WATCHED_FIELDS]
    + [
        StructField("last_observed_at", TimestampType(), nullable=True),
        StructField("last_money_domain", StringType(), nullable=True),
    ]
)


def _money_domain(currency: int | None, source_endpoint: str) -> str:
    """A row's money fields are only safely comparable to a prior row's if they're in the
    same "domain": a specific known currency if the row reports one (priceoverview,
    orderbook), or else the specific source endpoint (search_render never reports a
    currency — its `sell_price` empirically looks USD-cents-scaled, but that's unconfirmed,
    see the module docstring's Silver normalization table). Two `search_render`
    observations are comparable to each other; a `search_render` observation is NOT
    assumed comparable to a `priceoverview` or `orderbook` one just because both happen to
    be null/None on the currency field — that gap is exactly what let a real bug through
    on the first attempt at this fix (see docs/DECISIONS.md, 2026-08-21). `currency` must
    already be cleaned of NaN (pass None, not NaN) by the caller."""
    if currency is not None:
        return f"currency:{currency}"
    return f"endpoint:{source_endpoint}"

CHANGE_EVENT_SCHEMA = StructType([
    StructField("app_id", IntegerType()),
    StructField("market_hash_name", StringType()),
    StructField("field", StringType()),
    StructField("previous", DoubleType()),
    StructField("current", DoubleType()),
    StructField("delta", DoubleType()),
    StructField("pct_change", DoubleType(), nullable=True),
    StructField("observed_at", TimestampType()),
])

_SEARCH_RENDER_PAYLOAD_SCHEMA = StructType([
    StructField("results", ArrayType(StructType([
        StructField("hash_name", StringType()),
        StructField("sell_price", LongType()),
        StructField("sell_listings", LongType()),
    ])))
])

_PRICEOVERVIEW_PAYLOAD_SCHEMA = StructType([
    StructField("lowest_price", StringType()),
    StructField("median_price", StringType()),
    StructField("volume", StringType()),
])

_ORDERBOOK_PAYLOAD_SCHEMA = StructType([
    StructField("data", StructType([
        StructField("success", BooleanType()),
        StructField("data", StructType([
            StructField("amtMaxBuyOrder", LongType()),
            StructField("amtMinSellOrder", LongType()),
            StructField("eCurrency", IntegerType()),
            StructField("cBuyOrders", LongType()),
            StructField("cSellOrders", LongType()),
            StructField("rgCompactBuyOrders", ArrayType(LongType())),
            StructField("rgCompactSellOrders", ArrayType(LongType())),
        ])),
    ]))
])

_parse_price_udf = udf(parse_price_string, LongType())
_parse_volume_udf = udf(parse_volume_string, LongType())


def read_raw_envelopes(spark: SparkSession, bootstrap_servers: str) -> DataFrame:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", KAFKA_RAW_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )
    value_str = col("value").cast("string")
    return raw.select(
        col("key").cast("string").alias("kafka_key"),
        expr("get_json_object(CAST(value AS STRING), '$.endpoint')").alias("endpoint"),
        expr("CAST(get_json_object(CAST(value AS STRING), '$.app_id') AS INT)").alias("app_id"),
        expr("CAST(get_json_object(CAST(value AS STRING), '$.currency') AS INT)").alias("request_currency"),
        to_timestamp(expr("get_json_object(CAST(value AS STRING), '$.observed_at')")).alias("observed_at"),
        expr("get_json_object(CAST(value AS STRING), '$.raw_payload')").alias("raw_payload_json"),
    )


def _hash_name_from_key(df: DataFrame) -> DataFrame:
    return df.withColumn("market_hash_name", expr("substring(kafka_key, instr(kafka_key, ':') + 1)"))


def normalize_search_render(envelope_df: DataFrame) -> DataFrame:
    parsed = (
        envelope_df.filter(col("endpoint") == "search_render")
        .withColumn("payload", from_json(col("raw_payload_json"), _SEARCH_RENDER_PAYLOAD_SCHEMA))
        .select("app_id", "observed_at", explode(col("payload.results")).alias("result"))
    )
    return parsed.select(
        col("app_id"),
        col("result.hash_name").alias("market_hash_name"),
        col("observed_at"),
        col("result.sell_price").cast("long").alias("lowest_sell"),
        lit(None).cast("long").alias("highest_buy"),
        col("result.sell_listings").cast("long").alias("sell_listings"),
        lit(None).cast("long").alias("volume"),
        lit(None).cast("int").alias("currency"),
        lit("search_render").alias("source_endpoint"),
    )


def normalize_priceoverview(envelope_df: DataFrame) -> DataFrame:
    parsed = _hash_name_from_key(
        envelope_df.filter(col("endpoint") == "priceoverview").withColumn(
            "payload", from_json(col("raw_payload_json"), _PRICEOVERVIEW_PAYLOAD_SCHEMA)
        )
    )
    return parsed.select(
        col("app_id"),
        col("market_hash_name"),
        col("observed_at"),
        _parse_price_udf(col("payload.lowest_price"), col("request_currency")).alias("lowest_sell"),
        lit(None).cast("long").alias("highest_buy"),
        lit(None).cast("long").alias("sell_listings"),
        _parse_volume_udf(col("payload.volume")).alias("volume"),
        col("request_currency").alias("currency"),
        lit("priceoverview").alias("source_endpoint"),
    )


def normalize_orderbook(envelope_df: DataFrame) -> DataFrame:
    parsed = _hash_name_from_key(
        envelope_df.filter(col("endpoint") == "orderbook").withColumn(
            "payload", from_json(col("raw_payload_json"), _ORDERBOOK_PAYLOAD_SCHEMA)
        )
    )
    inner = col("payload.data.data")
    return parsed.select(
        col("app_id"),
        col("market_hash_name"),
        col("observed_at"),
        inner["amtMinSellOrder"].alias("lowest_sell"),
        inner["amtMaxBuyOrder"].alias("highest_buy"),
        inner["cSellOrders"].alias("sell_listings"),
        lit(None).cast("long").alias("volume"),
        inner["eCurrency"].alias("currency"),
        lit("orderbook").alias("source_endpoint"),
    )


def normalize(envelope_df: DataFrame) -> DataFrame:
    return (
        normalize_search_render(envelope_df)
        .unionByName(normalize_priceoverview(envelope_df))
        .unionByName(normalize_orderbook(envelope_df))
        .filter(col("market_hash_name").isNotNull() & (col("market_hash_name") != ""))
    )


def detect_changes(key, pdf_iter, state: GroupState):  # type: ignore[no-untyped-def]
    """Money-domain-aware (bug found and fixed TWICE, docs/DECISIONS.md 2026-08-21 — the
    first fix wasn't strict enough, see below). Watched fields are money amounts, and
    different source endpoints' money fields are not safely comparable: `search_render`
    never reports a currency (its `sell_price` empirically looks USD-cents-scaled, but
    that's unconfirmed — see the module docstring's Silver table), `priceoverview`'s is
    whatever we requested (currently always USD), and `orderbook`'s `eCurrency` is
    server-inferred and NOT currently controllable (Phase 1 finding).

    First attempt at this fix only compared *currency codes*, treating a null currency
    (search_render) as compatible with anything. That let a real bug through: an item
    whose baseline was built entirely from search_render observations (currency always
    null, so the tracked baseline currency stayed null) got diffed against a later
    orderbook observation (currency=8, JPY) without ever tripping the "conflict" check,
    since null-vs-real-currency wasn't treated as a conflict — producing another bogus
    "15,903% price move" (3878 USD-cents-ish -> 620600 JPY-minor-units-ish).

    Fixed properly via `_money_domain`: track a domain string, not a nullable currency —
    a specific currency when one is reported, otherwise the specific source endpoint. Two
    search_render observations share a domain; a search_render observation and an
    orderbook observation never do, regardless of either one's currency field being null.
    On a domain conflict, every field present in that row is silently reseeded (like a
    first observation) instead of emitting a bogus event.
    """
    import pandas as pd

    app_id, market_hash_name = key

    if state.exists:
        prev = state.get
        current_state = {f: prev[i] for i, f in enumerate(WATCHED_FIELDS)}
        current_domain = prev[len(WATCHED_FIELDS) + 1]
    else:
        current_state = {f: None for f in WATCHED_FIELDS}
        current_domain = None

    events: list[tuple[object, ...]] = []
    last_observed_at = None

    for pdf in pdf_iter:
        for _, row in pdf.sort_values("observed_at").iterrows():
            last_observed_at = row["observed_at"]
            row_currency = None if pd.isna(row["currency"]) else int(row["currency"])
            row_domain = _money_domain(row_currency, row["source_endpoint"])

            domain_conflict = current_domain is not None and row_domain != current_domain
            if domain_conflict:
                logger.warning(
                    "money domain changed for %s:%s (%s -> %s) — reseeding baseline, not diffing",
                    app_id, market_hash_name, current_domain, row_domain,
                )

            for field in WATCHED_FIELDS:
                new_val = row[field]
                if pd.isna(new_val):
                    continue  # this source endpoint doesn't carry this field
                old_val = current_state[field]
                if domain_conflict:
                    current_state[field] = new_val
                    continue
                if old_val is not None and new_val == old_val:
                    continue
                if old_val is not None:
                    delta = float(new_val) - float(old_val)
                    pct_change = (delta / old_val * 100) if old_val != 0 else None
                    events.append((
                        int(app_id), market_hash_name, field,
                        float(old_val), float(new_val), delta, pct_change,
                        row["observed_at"],
                    ))
                current_state[field] = new_val

            current_domain = row_domain

    state.update(tuple(current_state[f] for f in WATCHED_FIELDS) + (last_observed_at, current_domain))

    columns = ["app_id", "market_hash_name", "field", "previous", "current", "delta", "pct_change", "observed_at"]
    return iter([pd.DataFrame(events, columns=columns)])


def build_change_stream(spark: SparkSession, bootstrap_servers: str) -> DataFrame:
    raw = read_raw_envelopes(spark, bootstrap_servers)
    unified = normalize(raw)
    return (
        unified.withWatermark("observed_at", WATERMARK)
        .groupBy("app_id", "market_hash_name")
        .applyInPandasWithState(
            detect_changes,
            outputStructType=CHANGE_EVENT_SCHEMA,
            stateStructType=STATE_SCHEMA,
            outputMode="append",
            timeoutConf=GroupStateTimeout.NoTimeout,
        )
    )


def run(bootstrap_servers: str, checkpoint_location: str) -> None:
    spark = SparkSession.builder.appName("steam-market-cdc").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    changes = build_change_stream(spark, bootstrap_servers)

    query = (
        changes.selectExpr(
            "CAST(market_hash_name AS STRING) AS key",
            "to_json(struct(*)) AS value",
        )
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("topic", KAFKA_CHANGES_TOPIC)
        .option("checkpointLocation", checkpoint_location)
        .outputMode("append")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    import os

    run(
        bootstrap_servers=os.environ.get("SPARK_KAFKA_BOOTSTRAP", "redpanda:9092"),
        checkpoint_location=os.environ.get("CDC_CHECKPOINT_LOCATION", "/tmp/cdc_checkpoint"),
    )
