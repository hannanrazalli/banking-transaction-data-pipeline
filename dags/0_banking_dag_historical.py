from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from include.ingestion.generators.postgres_historical import historical_to_postgres

default_args = {
    'owner': 'Hannan_Razalli',
    'depends_on_past': False,
    'retries': 2,
}

with DAG(
    dag_id='1_Postgres_Historical_Gen',
    default_args = default_args,
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