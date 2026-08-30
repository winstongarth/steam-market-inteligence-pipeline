# Runbook

Cold start, service URLs, dbt run order, and the config values worth knowing from
`.env.example`.

---

## Cold start

```bash
cp .env.example .env   # adjust if needed — defaults work for local docker-compose

docker compose up -d
docker exec steam-redpanda rpk topic create market.raw.v1 market.changes.v1

uv run python -m ingest.scheduler          # poller -> Kafka
uv run python -m streaming.bronze_writer   # Kafka -> Bronze (Parquet on MinIO)
# The Spark CDC job runs inside the spark container, not on the host — see
# docs/DECISIONS.md ("Streaming / CDC") for the spark-submit invocation and why.

cd dbt && dbt seed && dbt run && dbt snapshot && dbt test
uv run python -m analytics.anomaly            # writes main.mart_anomaly
```

Full test suite: `uv run pytest -q`. Type checking: `uv run mypy ingest` (strict) and
`uv run mypy streaming analytics --ignore-missing-imports`.

## dbt run order, and why

```bash
dbt seed                              # reference data first — models join against it
dbt run --exclude dim_item            # staging + intermediate + non-snapshot-dependent marts
dbt snapshot                          # dim_item_snapshot needs stg_item_attributes, which
                                       # only exists after the run above
dbt run --select dim_item             # dim_item reads the snapshot, so it runs last
dbt test
```

The Airflow DAG (`airflow/dags/steam_market_batch.py`) encodes this same ordering
constraint in its task graph — `dbt_snapshot` depends on staging models already existing,
and `dim_item` depends on the snapshot. The plain `dbt run && dbt snapshot` shortcut shown
in "Cold start" above works too, as long as staging models build before the snapshot runs,
which that sequence already guarantees for a from-scratch build.

## Service URLs and ports

| Service | URL / port | Notes |
|---|---|---|
| Airflow webserver | http://localhost:8081 | `admin` / `admin` |
| Redpanda Console | http://localhost:8080 | Kafka topic browser |
| Redpanda (external) | `localhost:19092` | Kafka bootstrap for host-side scripts |
| MinIO API | http://localhost:9000 | S3-compatible; console on `9001` |
| `postgres-airflow` | internal only | Airflow's own metadata DB, not published to the host |
| Webhook receiver | `localhost:9109` | Alert delivery target for `quality/alerting.py` |

## Config reference (`.env.example`)

| Variable | Default | Notes |
|---|---|---|
| `STEAM_RPS` | `0.5` | Global rate limit, measured in `docs/FINDINGS.md` — do not raise without re-measuring |
| `STEAM_USER_AGENT` | contact-bearing UA string | Deliberate scraping-etiquette identifier — keep the contact address real |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:19092` | Host-side default; containers use `redpanda:9092` |
| `S3_ENDPOINT_URL` / `S3_BUCKET` / `S3_ACCESS_KEY` / `S3_SECRET_KEY` | MinIO local defaults | Swap for real S3 credentials outside local dev |
