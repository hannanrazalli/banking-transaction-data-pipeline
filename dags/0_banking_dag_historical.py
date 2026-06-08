from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from include.ingestion.generators.postgres_historical import historical_to_postgres

with DAG(
    dag_id='1_Postgres_Historical_Gen',
    schedule=None,
    start_date=datetime(2026, 5, 1),
    catchup=False
) as dag:

    run_historical_generator = PythonOperator(
        task_id='Populate_Historical_Postgres',
        python_callable=historical_to_postgres,
        op_kwargs={
            'start_date': '2026-05-01',
            'end_date': '2026-06-05'
        }
    )

    run_historical_generator