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
        """Set up MotherDuck token if available."""
        token = os.getenv("MOTHERDUCK_TOKEN")
        if token:
            os.environ["motherduck_token"] = token

    def log_stop_orders(self, df: pl.DataFrame) -> None:
        """
        Add stop orders to the database.
        """

        self.conn.execute(
            """INSERT INTO stop_orders (
                created_at,
                stop_price,
                qty,
                symbol,
                side,
                type,
                account_id,
                trade_id
            )
            SELECT * FROM df
            """
        )

    def get_account_ids(self) -> pl.DataFrame:
        """
        Get account ids.
        """

        return self.conn.execute(
            """
            SELECT id, account_number
            FROM accounts
            """
        ).pl()

    def get_executions(self) -> pl.DataFrame:
        """
        Get executions.
        """

        return self.conn.execute(
            """
            SELECT *
            FROM executions
            """
        ).pl()

    def log_executions(self, df: pl.DataFrame) -> None:
        """
        Add executions to the database.
        """

        self.conn.execute(
            """INSERT INTO executions (
                order_id,
                execution_id,
                created_at,
                filled_at,
                filled_avg_price,
                filled_qty,
                status,
                symbol,
                side,
                position_intent,
                account_id,
                trade_id
            )
            SELECT * FROM df
            """
        )

    def log_account_info(self, df: pl.DataFrame) -> None:
        """
        Add account info to the database.
        """

        self.conn.execute(
            """
            INSERT INTO accounts (account_number, currency, type)
            SELECT account_number, currency, type
            FROM df
            ON CONFLICT (account_number) DO NOTHING
            """
        )

    def log_account_snapshots(self, df: pl.DataFrame) -> None:
        """
        Add account snapshots to the database.
        """

        self.conn.execute(
            """
            INSERT INTO account_snapshots (
                account_id,
                equity,
                date
            )
            SELECT account_id, equity, date
            FROM df
            ON CONFLICT (account_id, date) DO NOTHING
            """
        )