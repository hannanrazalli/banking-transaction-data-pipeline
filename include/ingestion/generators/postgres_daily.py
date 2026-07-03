import uuid
import logging
import numpy as np
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

def daily_to_postgres(execution_date: str, postgres_conn_id: str = 'postgres_default') -> None:
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    
    account_pool = [f"ACC_{1000 + i}" for i in range(1000)]
    status_pool = ["ACTIVE", "SUSPENDED", "DORMANT", "VIP"]
    tier_pool = ["BRONZE", "SILVER", "GOLD", "PLATINUM"]
    
    try:
        num_accs = 15
        df_acc = pd.DataFrame({
            "account_id": np.random.choice(account_pool, num_accs, replace=False),
            "customer_name": [f"Customer_{i}" for i in np.random.randint(1000, 9999, num_accs)],
            "account_status": np.random.choice(status_pool, num_accs, p=[0.50, 0.20, 0.20, 0.10]),
            "account_tier": np.random.choice(tier_pool, num_accs),
            "credit_limit": np.random.choice([5000, 10000, 20000, 50000], num_accs).astype(float),
            "updated_at": [f"{execution_date} 12:00:00"] * num_accs
        })
        
        corrupt_acc_idx = np.random.choice(num_accs, size=1, replace=False)
        df_acc.loc[corrupt_acc_idx, "credit_limit"] = None

        num_txns = 1000
        df_txn = pd.DataFrame({
            "transaction_id": [str(uuid.uuid4()) for _ in range(num_txns)],
            "account_id": np.random.choice(account_pool, num_txns),
            "amount": np.round(np.random.uniform(5.0, 10000.0, num_txns), 2).astype(float),
            "currency": np.random.choice(["MYR", "USD", "SGD", "EUR"], num_txns, p=[0.4, 0.3, 0.2, 0.1]),
            "transaction_type": np.random.choice(["DEPOSIT", "WITHDRAWAL", "TRANSFER", "PAYMENT"], num_txns),
            "transaction_date": [f"{execution_date} 12:00:00"] * num_txns
        })
        
        corrupt_txn_idx = np.random.choice(num_txns, size=int(num_txns * 0.01), replace=False)
        df_txn.loc[corrupt_txn_idx, "amount"] = None

        pg_hook.insert_rows(
            table="accounts",
            rows=df_acc.where(pd.notnull(df_acc), None).values.tolist(),
            target_fields=df_acc.columns.tolist(),
            replace=True,
            replace_index="account_id"
        )
        
        pg_hook.insert_rows(
            table="transactions",
            rows=df_txn.where(pd.notnull(df_txn), None).values.tolist(),
            target_fields=df_txn.columns.tolist()
        )
        
        logger.info(f"Successfully populated PostgreSQL with daily data for {execution_date}")
        
    except Exception as e:
        logger.error(f"Failed to populate daily data to Postgres for {execution_date}: {e}")
        raise e