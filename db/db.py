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

    def __exit__(self, exc_type, exc_val, exc_tb):
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

    def add_unique_tickers(self, df: pl.DataFrame) -> None:
        self.conn.execute("""
            INSERT INTO tickers (ticker)
            SELECT ticker from df
            ON CONFLICT (ticker) DO NOTHING
        """)

    def add_to_watchlist(self, df: pl.DataFrame, type: str) -> None:
        self.conn.execute(f"""
            INSERT INTO watchlist_events (ticker_id, wl_type, event_type)
            SELECT
                t.id,
                '{type}',
                df.event_type
            FROM df
            JOIN tickers t ON df.ticker = t.ticker
        """)

    def add_tickers(self, df: pl.DataFrame, type: str) -> None:
        """
        Add tickers to the database.

        Args:
            df: Polars DataFrame with 'ticker' and 'event_type' columns

        """
        # Register the DataFrame
        self.conn.register("df", df)

        # Step 1: Insert tickers with sequence-generated IDs
        self.add_unique_tickers(df)

        # Step 2: Insert into watchlist_events with sequence-generated IDs
        self.add_to_watchlist(df, type)
