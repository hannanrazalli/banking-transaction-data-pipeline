# Banking Transaction Data Pipeline (Medallion Architecture)

End-to-end data pipeline processing daily banking transactions, account changes, and forex rates. Built for a local environment using Airflow, dbt, and BigQuery with a medallion architecture (Bronze -> Silver -> Gold).

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

**2. Data-aware scheduling with Airflow Datasets.** The `medallion_banking` DAG triggers automatically only after the `s3_to_bq_daily` DAG completes. No cron-based polling. A gatekeeper task ensures all three load tasks succeed before emitting the dataset, preventing the dbt DAG from running on partial data.

**3. Quarantine pattern for data quality.** Corrupted records (null keys, missing amounts) are flagged in staging and routed to quarantine tables (`int_transactions_qrt`, `int_forex_qrt`) instead of being silently dropped. The Gold layer only consumes clean records.

**4. SCD Type 2 and idempotency.** `dim_accounts` tracks historical changes to customer attributes using surrogate keys, `valid_from`, `valid_to`, and `is_current` flags. Only records where attributes actually changed trigger a new version (no unnecessary churn). All incremental models use `MERGE` with unique keys, so re-running for the same date won't duplicate data.

**5. dbt tests for schema validation.** Every model has `not_null`, `unique`, and `accepted_values` tests defined in `schema.yml` files. Run `dbt test` to validate data quality across all layers.

**6. Native BigQuery tables.** Raw data lands in BigQuery native tables (not external tables) for better query performance and lower costs during dbt transformations.

## Repository Structure

```
.
├── .github/workflows/ci.yml        # PR checks: dbt compile + dbt test
├── dags/
│   ├── 0_generate_daily_data.py     # Mock daily data to PostgreSQL
│   ├── 0_generate_historical_data.py# Mock historical data to PostgreSQL
│   ├── 1_db_to_s3_daily.py          # Daily extract from Postgres to S3
│   ├── 1_db_to_s3_historical.py     # Historical extract from Postgres to S3
│   ├── 2_s3_to_bq_daily.py          # Daily load from S3 to BigQuery (emits Dataset)
│   ├── 2_s3_to_bq_historical.py     # Historical load from S3 to BigQuery
│   └── 3_medallion_banking.py       # dbt transformation DAG (Cosmos, Dataset-triggered)
├── include/
│   ├── ingestion/
│   │   ├── api/                     # Forex API (daily + historical)
│   │   ├── generators/              # PostgreSQL mock data generators
│   │   └── loaders/                 # S3-to-BigQuery loaders
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
GCP_PROJECT=your-project
GCP_DATASET=bronze
GCP_LOCATION=asia-southeast1
FOREX_API_KEY=your_key
IS_PRODUCTION=False
```

Start Airflow:

```bash
astro dev start
```

Access the UI at `http://localhost:8080` (admin/admin).

## CI

This project uses GitHub Actions to run `dbt compile` and `dbt test` on every pull request, catching SQL errors and schema violations before merge.

## Pipeline Visualizations

### dbt Lineage

![dbt Lineage](images/Medallion.png)

### Airflow DAGs

![Airflow DAGs](images/Airflow_DAG.png)
