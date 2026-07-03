# Banking Transaction Data Pipeline (Medallion Architecture)

End-to-end data pipeline processing daily banking transactions, account changes, and forex rates. Built for a local environment using Airflow, dbt, and BigQuery with a medallion architecture (Bronze → Silver → Gold).

## Architecture & Tech Stack

![Architecture](images/Architecture.png)

| Component | Technology |
|---|---|
| Source system | PostgreSQL (containerized) |
| Orchestration | Apache Airflow (Astronomer Cosmos) |
| Data warehouse | Google BigQuery (native tables) |
| Object storage | AWS S3 |
| Transformation | dbt (Data Build Tool) |
| Languages | Python, SQL (Jinja) |

## Key Design Decisions

**1. Decoupled EL and T.** Ingestion and transformation are separate DAGs. If dbt fails (schema drift, bad data), the ingestion DAG continues landing raw data in the Bronze layer, isolating failure domains.

**2. Data-aware scheduling with Airflow Datasets.** The `3_medallion_banking` DAG triggers automatically only after an upstream DAG completes. Both daily and historical load DAGs emit a Dataset via a gatekeeper task, ensuring the dbt DAG never runs on partial data.

**3. Quarantine pattern for data quality.** Corrupted records (null keys, missing amounts, null exchange rates) are flagged in staging and routed to quarantine tables instead of being silently dropped. The Gold layer only consumes clean records.

**4. SCD Type 2 and idempotency.** `dim_accounts` tracks historical changes to customer attributes using surrogate keys, `valid_from`, `valid_to`, and `is_current` flags. Only records where attributes actually changed trigger a new version. All incremental models use `MERGE` with `unique_key`, so re-running for the same date won't duplicate data.

**5. Layered dbt testing strategy.** Staging models test only `_record_status` (must be non-null). Intermediate models hold strict tests (`not_null`, `unique`, `accepted_values`) on cleaned data. Mart models test referential integrity and business-schema constraints. Calculated fields depending on external API data (e.g., `amount_usd`) are validated but not tested with `not_null`.

**6. Native BigQuery tables.** Raw data lands in BigQuery native tables (not external tables) for better query performance and lower costs during dbt transformations.

## Repository Structure

```
.
├── .github/workflows/ci.yml        # PR checks: dbt parse
├── dags/
│   ├── 0_banking_dag_daily.py      # [0_generate_daily_data] Mock daily data to PostgreSQL
│   ├── 0_banking_dag_historical.py # [0_generate_historical_data] Mock historical data
│   ├── 1_db_to_s3_daily.py         # [1_db_to_s3_daily] Extract Postgres → S3 (parameterized queries)
│   ├── 1_db_to_s3_historical.py    # [1_db_to_s3_historical] Historical extract Postgres → S3
│   ├── 2_s3_to_bq_daily.py         # [2_s3_to_bq_daily] Load S3 → BigQuery (emits Dataset)
│   ├── 2_s3_to_bq_historical.py    # [2_s3_to_bq_historical] Historical load S3 → BigQuery (emits Dataset)
│   └── 3_medallion_banking.py      # [3_medallion_banking] dbt transform (Dataset-triggered)
├── include/
│   ├── ingestion/
│   │   ├── api/                     # Forex API (daily + historical, with timeout)
│   │   ├── generators/              # PostgreSQL mock data generators
│   │   └── loaders/                 # S3-to-BigQuery loaders (batched, error-handled)
│   └── dbt/banking_data_pipeline/
│       ├── models/
│       │   ├── 1_staging/           # Ephemeral: type casting + record status
│       │   ├── 2_intermediate/      # Incremental: dedup, quarantine, FX join
│       │   └── 3_marts/             # Incremental: star schema + aggregates
│       ├── macros/                  # audit_columns, generate_schema_name
│       └── tests/                   # dbt test schemas (not_null, unique, accepted_values)
├── tests/dags/                      # Airflow DAG integrity tests (pytest)
├── requirements.txt
└── Dockerfile
```

## Local Development

Requires active AWS credentials (S3) and a GCP service account (BigQuery).

```bash
git clone https://github.com/hannanrazalli/banking-transaction-data-pipeline.git
cd banking-transaction-data-pipeline
```

Create a `.env` file:

```env
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_DEFAULT_REGION=ap-southeast-1
S3_BUCKET_NAME=your-bucket
POSTGRES_CONN_ID=postgres_default
AWS_CONN_ID=aws_default
GCP_PROJECT=your-project
GCP_DATASET=bronze
GCP_LOCATION=asia-southeast1
FOREX_API_KEY=your_key
HISTORICAL_START_DATE=2026-06-01
HISTORICAL_END_DATE=2026-07-01
IS_PRODUCTION=False
```

Start Airflow:

```bash
astro dev start
```

Access the UI at `http://localhost:8080` (admin/admin).

## CI

This project uses GitHub Actions to run `dbt parse` on every pull request, catching SQL compilation errors before merge (no credentials needed — uses a mock BigQuery profile).

## Pipeline Visualizations

### dbt Lineage

![dbt Lineage](images/Medallion.png)

### Airflow DAGs

![Airflow DAGs](images/Airflow_DAG.png)
