import os

import duckdb
import polars as pl
from dotenv import load_dotenv

load_dotenv()


class DuckDBConnector:
    def __init__(self):
        self._setup_motherduck_token()
        self.conn = duckdb.connect("md:stocksdb")

    def __enter__(self):
        self.conn.begin()
        return self

    def __exit__(self, exc_type, _exc_val, _exc_tb):
        if exc_type is not None:
            self.conn.rollback()
        else:
            self.conn.commit()
        self.conn.close()
        return False

    def _setup_motherduck_token(self) -> None:
        token = os.getenv("MOTHERDUCK_TOKEN")
        if token:
            os.environ["motherduck_token"] = token

    def log_account_info(self, df: pl.DataFrame) -> None:
        self.conn.execute(
            """
            INSERT INTO accounts (account_number, currency, type)
            SELECT account_number, currency, type
            FROM df
            ON CONFLICT (account_number) DO NOTHING
            """
        )

    def get_account_number(self, account_number: str) -> str:
        return self.conn.execute(
            "SELECT account_number FROM accounts WHERE account_number = ?",
            [account_number],
        ).fetchone()[0]

    def log_account_snapshots(self, df: pl.DataFrame) -> None:
        self.conn.execute(
            """
            INSERT INTO account_snapshots (account_number, equity, date)
            SELECT account_number, equity, date
            FROM df
            ON CONFLICT (account_number, date) DO NOTHING
            """
        )

    def log_executions(self, df: pl.DataFrame) -> None:
        self.conn.execute(
            """INSERT INTO executions (
                execution_id,
                order_id,
                created_at,
                filled_at,
                filled_avg_price,
                filled_qty,
                status,
                symbol,
                side,
                position_intent,
                account_number,
                trade_id
            )
            SELECT * FROM df
            ON CONFLICT (execution_id) DO NOTHING
            """
        )

    def get_executions_count(self) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM executions").fetchone()[0]

    def get_stop_orders_count(self) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM stop_orders").fetchone()[0]

    def get_executions(self) -> pl.DataFrame:
        return self.conn.execute("SELECT * FROM executions").pl()

    def log_stop_orders(self, df: pl.DataFrame) -> None:
        self.conn.execute(
            """INSERT INTO stop_orders (
                id,
                created_at,
                stop_price,
                qty,
                symbol,
                side,
                type,
                account_number,
                trade_id
            )
            SELECT * FROM df
            ON CONFLICT (id) DO NOTHING
            """
        )

    def get_stop_orders(self) -> pl.DataFrame:
        return self.conn.execute("SELECT * FROM stop_orders").pl()
