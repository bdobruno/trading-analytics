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
        conn.execute("DROP TABLE IF EXISTS executions CASCADE;")
        conn.execute("DROP TABLE IF EXISTS stop_orders CASCADE;")
        conn.execute("DROP TABLE IF EXISTS account_snapshots CASCADE;")
        conn.execute("DROP TABLE IF EXISTS accounts CASCADE;")

        print("Creating tables...")

        # Accounts table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS accounts (
                account_number VARCHAR PRIMARY KEY,
                currency VARCHAR,
                type VARCHAR CHECK (type IN ('paper', 'live'))
            );
        """)

        # Executions table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS executions (
                execution_id VARCHAR PRIMARY KEY,
                order_id VARCHAR NOT NULL,
                parent_order_id VARCHAR,
                created_at TIMESTAMPTZ NOT NULL,
                filled_at TIMESTAMPTZ NOT NULL,
                filled_avg_price DOUBLE,
                filled_qty INTEGER,
                status VARCHAR NOT NULL,
                symbol VARCHAR NOT NULL,
                side VARCHAR NOT NULL,
                position_intent VARCHAR NOT NULL,
                account_number VARCHAR NOT NULL,
                trade_id INTEGER NOT NULL,
                FOREIGN KEY (account_number) REFERENCES accounts(account_number)
            );
        """)

        # Account snapshots table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS account_snapshots (
                account_number VARCHAR,
                equity DOUBLE,
                date DATE,
                PRIMARY KEY (account_number, date),
                FOREIGN KEY (account_number) REFERENCES accounts(account_number)
            );
        """)

        # Stop orders table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS stop_orders (
                id VARCHAR PRIMARY KEY,
                created_at TIMESTAMP NOT NULL,
                stop_price DOUBLE NOT NULL,
                qty INTEGER NOT NULL,
                symbol VARCHAR NOT NULL,
                side VARCHAR NOT NULL,
                type VARCHAR NOT NULL,
                account_number VARCHAR NOT NULL,
                trade_id INTEGER NOT NULL,
                FOREIGN KEY (account_number) REFERENCES accounts(account_number)
            );
        """)

        print("Database setup complete!")
    except Exception as e:
        print(f"Error during setup: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    setup_database()
