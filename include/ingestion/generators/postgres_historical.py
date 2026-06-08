import uuid
import logging
import numpy as np
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

def historical_to_postgres(start_date: str, end_date: str) -> None:
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    account_pool = [f"ACC_{1000 + i}" for i in range(1000)]
    status_pool = ["ACTIVE", "SUSPENDED", "DORMANT", "VIP"]
    tier_pool = ["BRONZE", "SILVER", "GOLD", "PLATINUM"]
    
    try:
        dates = pd.date_range(start=start_date, end=end_date)
        
        for dt in dates:
            date_str = dt.strftime('%Y-%m-%d')
            
            num_accs = 15
            df_acc = pd.DataFrame({
                "account_id": np.random.choice(account_pool, num_accs, replace=False),
                "customer_name": [f"Customer_{i}" for i in np.random.randint(1000, 9999, num_accs)],
                "account_status": np.random.choice(status_pool, num_accs, p=[0.50, 0.20, 0.20, 0.10]),
                "account_tier": np.random.choice(tier_pool, num_accs),
                "credit_limit": np.random.choice([5000, 10000, 20000, 50000], num_accs).astype(float),
                "updated_at": [f"{date_str} 12:00:00"] * num_accs
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
                "transaction_date": [f"{date_str} 12:00:00"] * num_txns
            })
            
            corrupt_txn_idx = np.random.choice(num_txns, size=int(num_txns * 0.01), replace=False)
            df_txn.loc[corrupt_txn_idx, "amount"] = None

            for _, row in df_acc.iterrows():
                credit_limit_val = 'NULL' if pd.isnull(row['credit_limit']) else row['credit_limit']
                insert_query = f"""
                    INSERT INTO accounts (account_id, customer_name, account_status, account_tier, credit_limit, updated_at)
                    VALUES ('{row['account_id']}', '{row['customer_name']}', '{row['account_status']}', '{row['account_tier']}', {credit_limit_val}, '{row['updated_at']}')
                    ON CONFLICT (account_id) DO NOTHING;
                """
                pg_hook.run(insert_query)
            
            pg_hook.insert_rows(
                table="transactions",
                rows=df_txn.where(pd.notnull(df_txn), None).values.tolist(),
                target_fields=df_txn.columns.tolist()
            )
            
        logger.info(f"Successfully populated PostgreSQL with historical data from {start_date} to {end_date}")
        
    except Exception as e:
        logger.error(f"Failed to populate historical data to Postgres: {e}")
        raise e