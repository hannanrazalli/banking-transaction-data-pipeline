# Event-Driven Banking Data Pipeline (Medallion Architecture)

## 📌 Overview
This repository contains an end-to-end, event-driven data pipeline designed to process daily banking transactions and forex data. Built with scalability and fault-tolerance in mind, the pipeline orchestrates data extraction from cloud storage (AWS S3) and transforms it within a data warehouse (Google BigQuery) using a strict Medallion Architecture (Bronze -> Silver -> Gold).

## 🏗️ Architecture & Tech Stack
*(Insert your Architecture Diagram here using Draw.io or Excalidraw)*

* **Orchestration:** Apache Airflow (Astronomer Cosmos)
* **Data Warehouse:** Google BigQuery (Native Tables)
* **Data Lake / Object Storage:** AWS S3
* **Transformation:** dbt (Data Build Tool)
* **Language:** Python, SQL (Jinja)

## ⚙️ Key Architectural Decisions

As a data engineering project aimed at production-grade reliability, several senior-level design patterns were implemented:

**1. Decoupled EL & T Pipelines**
Ingestion (Extract & Load) and Transformation workflows are physically separated into distinct DAGs. If the dbt transformation fails due to a schema drift, the ingestion DAG will continue to land raw data safely into the Bronze layer, preventing data loss and ensuring fault tolerance.

**2. Data-Aware Scheduling (Event-Driven)**
Instead of relying on fragile cron-based time schedules (Time-Driven), this pipeline utilizes **Airflow Datasets**. The `medallion_banking` DAG is configured to trigger *only* and *immediately* after the `s3_to_bq` ingestion DAG successfully completes. This prevents blind runs and guarantees data integrity.

**3. Idempotency & Incremental Processing**
* **Idempotency:** Re-running the pipeline multiple times for the same day will not duplicate records in the Gold layer. `MERGE/UPSERT` strategies are enforced using unique composite keys (`transaction_id`).
* **Incremental Loading:** Configured dbt materializations to perform full-refreshes in `dev` environments, but strict incremental loads in `prod` environments (`"full_refresh": not IS_PROD`), heavily optimizing BigQuery compute costs.

**4. Optimized BigQuery Storage**
Avoided BigQuery External Tables for transactional data. Instead, Python `BigQueryHook` (with `WRITE_APPEND`) is used to write data physically into BigQuery Native Tables at the Bronze layer, significantly improving query performance and enabling future clustering/partitioning.

## 📂 Repository Structure
```text
.
├── dags/
│   ├── historical_to_s3.py         # One-off backfill DAG
│   ├── historical_s3_to_bq.py      # One-off backfill DAG
│   ├── daily_to_s3.py              # Daily incremental extraction to S3
│   ├── s3_to_bq.py                 # Ingestion from S3 to BigQuery (Emits Airflow Dataset)
│   └── medallion_banking.py        # Astronomer Cosmos DAG for dbt models
├── include/
│   ├── ingestion/                  # Custom Python loaders and extractors
│   └── dbt/banking_data_pipeline/  # dbt project (models, macros, tests)
├── requirements.txt                # Python dependencies
└── Dockerfile                      # Astro Runtime custom image


🚀 How to Run Locally

**1. Clone the repository:**
git clone https://github.com/hannanrazalli/banking-transaction-data-pipeline.git
cd banking-transaction-data-pipeline

**2. Environment Variables:**
Create a .env file in the root directory and configure your cloud credentials securely (Do not commit your GCP JSON key).

**3. Start the Airflow Cluster:**
astro dev start

**4. Access Airflow UI:**
Navigate to http://localhost:8080 (Default credentials: admin/admin).


## 📊 Pipeline Visualizations & Proof of Execution

### 1. dbt Medallion Architecture Lineage Graph
This graph illustrates the modular dependency and data flow from raw staging tables to downstream analytical marts inside Google BigQuery:

![dbt Medallion Lineage Graph](image/airflow.png)

### 2. Airflow Production DAGs (Successful Runs)
Proof of execution for all 5 production and historical backfill DAGs running successfully within the Astro Runtime environment:

![Airflow DAG Success Run](image/medallion_graph.png)