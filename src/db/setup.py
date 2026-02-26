import os

import duckdb
from dotenv import load_dotenv

load_dotenv()


def setup_database():
    """Connect to MotherDuck and create schema"""

    motherduck_token = os.getenv("MOTHERDUCK_TOKEN")

    if not motherduck_token:
        raise ValueError("MOTHERDUCK_TOKEN environment variable not set")

    conn = duckdb.connect("md:stocksdb")

    try:
        print("Dropping tables in dependency order...")
        conn.execute("DROP TABLE IF EXISTS trades_stop_orders CASCADE;")
        conn.execute("DROP TABLE IF EXISTS trades_executions CASCADE;")
        conn.execute("DROP TABLE IF EXISTS trades CASCADE;")
        conn.execute("DROP TABLE IF EXISTS executions CASCADE;")
        conn.execute("DROP TABLE IF EXISTS stop_orders CASCADE;")
        conn.execute("DROP TABLE IF EXISTS account_snapshots CASCADE;")
        conn.execute("DROP TABLE IF EXISTS accounts CASCADE;")
        conn.execute("DROP TABLE IF EXISTS watchlist_events CASCADE;")
        conn.execute("DROP TABLE IF EXISTS tickers CASCADE;")

        print("Dropping sequences...")
        conn.execute("DROP SEQUENCE IF EXISTS executions_seq;")
        conn.execute("DROP SEQUENCE IF EXISTS accounts_seq;")
        conn.execute("DROP SEQUENCE IF EXISTS stop_orders_seq;")
        conn.execute("DROP SEQUENCE IF EXISTS watchlist_events_id_seq;")
        conn.execute("DROP SEQUENCE IF EXISTS tickers_id_seq;")

        print("Creating sequences...")
        conn.execute("CREATE SEQUENCE executions_seq START 1;")
        conn.execute("CREATE SEQUENCE accounts_seq START 1;")
        conn.execute("CREATE SEQUENCE stop_orders_seq START 1;")
        conn.execute("CREATE SEQUENCE watchlist_events_id_seq START 1;")
        conn.execute("CREATE SEQUENCE tickers_id_seq START 1;")

        print("Creating tables...")

        # Create tickers table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tickers (
                id INTEGER PRIMARY KEY DEFAULT nextval('tickers_id_seq'),
                ticker VARCHAR UNIQUE NOT NULL
            )
        """)

        # Watchlist events table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS watchlist_events (
                id INTEGER PRIMARY KEY DEFAULT nextval('watchlist_events_id_seq'),
                ticker_id INTEGER NOT NULL,
                wl_type VARCHAR NOT NULL CHECK (wl_type IN ('pets', 'gappers')),
                event_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                event_type VARCHAR NOT NULL CHECK (event_type IN ('add', 'removal')),
                FOREIGN KEY (ticker_id) REFERENCES tickers(id)
            )
        """)

        # Accounts table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS accounts (
                id INTEGER PRIMARY KEY DEFAULT nextval('accounts_seq'),
                account_number VARCHAR UNIQUE,
                currency VARCHAR,
                type VARCHAR CHECK (type IN ('paper', 'live'))
            );
        """)

        # Executions table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS executions (
                id INTEGER PRIMARY KEY DEFAULT nextval('executions_seq'),
                order_id VARCHAR NOT NULL,
                execution_id VARCHAR NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                filled_at TIMESTAMPTZ NOT NULL,
                filled_avg_price DOUBLE,
                filled_qty INTEGER,
                status VARCHAR NOT NULL,
                symbol VARCHAR NOT NULL,
                side VARCHAR NOT NULL,
                position_intent VARCHAR NOT NULL,
                account_id INTEGER NOT NULL,
                trade_id INTEGER NOT NULL,
                FOREIGN KEY (account_id) REFERENCES accounts(id)
            );
        """)

        # Account snapshots table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS account_snapshots (
                account_id INTEGER,
                equity DOUBLE,
                date DATE,
                PRIMARY KEY (account_id, date),
                FOREIGN KEY (account_id) REFERENCES accounts(id)
            );
        """)

        # Stop orders table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS stop_orders (
                id INTEGER PRIMARY KEY DEFAULT nextval('stop_orders_seq'),
                created_at TIMESTAMP NOT NULL,
                stop_price DOUBLE NOT NULL,
                qty INTEGER NOT NULL,
                symbol VARCHAR NOT NULL,
                side VARCHAR NOT NULL,
                type VARCHAR NOT NULL,
                account_id INTEGER NOT NULL,
                trade_id INTEGER NOT NULL,
                FOREIGN KEY (account_id) REFERENCES accounts(id)
            );
        """)

        # Create indexes for better query performance
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_watchlist_events_ticker_id
            ON watchlist_events(ticker_id)
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_watchlist_events_wl_type
            ON watchlist_events(wl_type)
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_watchlist_events_event_time
            ON watchlist_events(event_time)
        """)

        print("Database setup complete!")
    except Exception as e:
        print(f"Error during setup: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    setup_database()
