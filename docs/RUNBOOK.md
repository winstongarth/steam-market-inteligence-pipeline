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

## Dashboard (Grafana)

```bash
docker compose up -d postgres-marts grafana

cd dbt && dbt seed && dbt run && dbt snapshot && dbt run --select path:models/marts
uv run python -m analytics.anomaly            # writes main.mart_anomaly
uv run python -m analytics.export_to_grafana  # DuckDB marts -> postgres-marts
```

Open http://localhost:3000 (default `admin` / `admin`, or `GRAFANA_ADMIN_PASSWORD` — see
`.env.example`). Two dashboards are auto-provisioned under "Steam Market Intelligence":

- **CS2 — Anomaly Detection** — counts/severity by anomaly type, a severity-over-time
  scatter, and the full `mart_anomaly` table (z-score, spread-widening, volume-spike,
  crossed-book).
- **CS2 — Cross-Currency Pricing** — `pct_gap` (non-USD vs. nearest-in-time USD baseline)
  over time and by currency, plus the underlying ASOF-joined observations.

Both default their `app_id` template variable to `730` (CS2) via `DASHBOARD_APP_IDS` in
`.env.example` — the sync itself isn't CS2-only, so widening to another tracked game
(`ingest/config.py`'s `APP_IDS`) is a config change, not a code change. Dashboards read
from whatever was last synced, so an anomaly/FX dashboard with no rows just means the sync
step hasn't run yet against real data, not a broken dashboard. `postgres-marts` is a
disposable, full-refresh mirror — DuckDB stays the single source of truth, and the sync
runs automatically as the DAG's `sync_dashboard` task, right after `detect_anomalies`.

## Service URLs and ports

| Service | URL / port | Notes |
|---|---|---|
| Airflow webserver | http://localhost:8081 | `admin` / `admin` |
| Grafana | http://localhost:3000 | `admin` / `$GRAFANA_ADMIN_PASSWORD` (default `admin`) |
| Redpanda Console | http://localhost:8080 | Kafka topic browser |
| Redpanda (external) | `localhost:19092` | Kafka bootstrap for host-side scripts |
| MinIO API | http://localhost:9000 | S3-compatible; console on `9001` |
| `postgres-marts` (host) | `localhost:5433` | Published port; in-network services use `postgres-marts:5432` |
| `postgres-airflow` | internal only | Airflow's own metadata DB, not published to the host |
| Webhook receiver | `localhost:9109` | Alert delivery target for `quality/alerting.py` |

## Config reference (`.env.example`)

| Variable | Default | Notes |
|---|---|---|
| `STEAM_RPS` | `0.5` | Global rate limit, measured in `docs/FINDINGS.md` — do not raise without re-measuring |
| `STEAM_USER_AGENT` | contact-bearing UA string | Deliberate scraping-etiquette identifier — keep the contact address real |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:19092` | Host-side default; containers use `redpanda:9092` |
| `S3_ENDPOINT_URL` / `S3_BUCKET` / `S3_ACCESS_KEY` / `S3_SECRET_KEY` | MinIO local defaults | Swap for real S3 credentials outside local dev |
| `GRAFANA_POSTGRES_URL` | `localhost:5433` (host) / `postgres-marts:5432` (in-network) | The Airflow DAG's `sync_dashboard` task overrides this to the in-network host automatically |
| `GRAFANA_ADMIN_PASSWORD` | `admin` | Change for anything beyond local dev |
| `DASHBOARD_APP_IDS` | `730` (CS2) | Comma-separated `app_id`s to sync; unset to sync every tracked game in `ingest/config.py`'s `APP_IDS` |
