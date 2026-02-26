from datetime import date

import polars as pl
from alpaca.trading.models import TradeAccount

from src.db.db import DuckDBConnector


def inserting_account_info(
    db: DuckDBConnector, account: TradeAccount, account_type: str
) -> str:
    df = pl.DataFrame([account.model_dump()])

    df = df.select(
        pl.col("account_number"),
        pl.col("currency"),
        pl.lit(account_type).alias("type"),
    )

    db.log_account_info(df)

    print(f"Account upserted, account_number={account.account_number}")
    return account.account_number


def inserting_account_snapshot(
    db: DuckDBConnector, account: TradeAccount, account_number: str
) -> None:
    df = pl.DataFrame(
        {
            "account_number": [account_number],
            "equity": [float(account.equity) if account.equity is not None else None],
            "date": [date.today()],
        }
    )

    db.log_account_snapshots(df)
    print("Account snapshot inserted in DB")
