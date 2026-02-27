import os
import time

import polars as pl
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderStatus, OrderType, QueryOrderStatus
from alpaca.trading.models import Order, TradeAccount
from alpaca.trading.requests import GetOrdersRequest

from src.db.db import DuckDBConnector
from src.processes.log_account_info import (
    inserting_account_info,
    inserting_account_snapshot,
)
from src.processes.trade_id import (
    assign_trade_ids_to_executions,
    assign_trade_ids_to_stops,
)

POLL_INTERVAL = 60


def build_clients() -> list[dict]:
    return [
        {
            "client": TradingClient(
                api_key=os.environ["ALPACA_API_KEY_PAPER"],
                secret_key=os.environ["ALPACA_SECRET_KEY_PAPER"],
                paper=True,
            ),
            "type": "paper",
        },
    ]


def poll_account_info(
    db: DuckDBConnector, client: TradingClient, account_type: str
) -> tuple[TradeAccount, str]:
    alpaca_account = client.get_account()
    assert isinstance(alpaca_account, TradeAccount)
    account_number = inserting_account_info(db, alpaca_account, account_type)
    inserting_account_snapshot(db, alpaca_account, account_number)
    return alpaca_account, account_number


def fetch_all_orders(client: TradingClient) -> list[Order]:
    raw = client.get_orders(filter=GetOrdersRequest(status=QueryOrderStatus.ALL))
    return [o for o in raw if isinstance(o, Order)]  # type: ignore[union-attr]


def _flatten_orders(orders: list[Order]) -> list[tuple[Order, str | None]]:
    """Return (order, parent_id) tuples, unpacking legs from multi-leg orders."""
    flat: list[tuple[Order, str | None]] = []
    for o in orders:
        flat.append((o, None))
        if o.legs:
            parent_id = str(o.id)
            for leg in o.legs:
                flat.append((leg, parent_id))
    return flat


def get_executions(orders: list[Order], from_date: str) -> list[dict]:
    result = []
    for o, parent_id in _flatten_orders(orders):
        if (
            o.status == OrderStatus.FILLED
            and o.symbol is not None
            and o.created_at is not None
            and o.created_at.date().isoformat() >= from_date
        ):
            d = o.model_dump(mode="json")
            d["parent_order_id"] = parent_id
            result.append(d)
    return result


def get_stop_orders(orders: list[Order], from_date: str) -> list[dict]:
    return [
        o.model_dump(mode="json")
        for o, _ in _flatten_orders(orders)
        if o.order_type == OrderType.STOP
        and o.symbol is not None
        and o.created_at is not None
        and o.created_at.date().isoformat() >= from_date
    ]


def poll_account(db: DuckDBConnector, client: TradingClient, account_type: str) -> None:
    alpaca_account, account_number = poll_account_info(db, client, account_type)

    print(f"\n=== {account_type} | account_number={account_number} ===")
    print(f"equity={alpaca_account.equity} currency={alpaca_account.currency}")

    all_orders = fetch_all_orders(client)
    executions = get_executions(all_orders, "2026-02-26")
    stop_orders = get_stop_orders(all_orders, "2026-02-26")

    executions_with_tid = assign_trade_ids_to_executions(executions)
    stops_with_tid = assign_trade_ids_to_stops(stop_orders, executions_with_tid)

    if not executions_with_tid.is_empty():
        new_count = db.log_executions(
            executions_with_tid.select(
                [
                    pl.col("id").cast(pl.String).alias("execution_id"),
                    pl.col("client_order_id").alias("order_id"),
                    pl.col("parent_order_id"),
                    pl.col("created_at"),
                    pl.col("filled_at"),
                    pl.col("filled_avg_price"),
                    pl.col("filled_qty"),
                    pl.col("status"),
                    pl.col("symbol"),
                    pl.col("side"),
                    pl.col("position_intent"),
                    pl.lit(account_number).alias("account_number"),
                    pl.col("trade_id"),
                ]
            )
        )
        print(f"Executions: {new_count} new / {len(executions_with_tid)} total")

    if not stops_with_tid.is_empty():
        new_count = db.log_stop_orders(
            stops_with_tid.select(
                [
                    pl.col("id").cast(pl.String),
                    pl.col("created_at"),
                    pl.col("stop_price"),
                    pl.col("qty"),
                    pl.col("symbol"),
                    pl.col("side"),
                    pl.col("type"),
                    pl.lit(account_number).alias("account_number"),
                    pl.col("trade_id"),
                ]
            )
        )
        print(f"Stop orders: {new_count} new / {len(stops_with_tid)} total")


def run() -> None:
    accounts = build_clients()

    while True:
        with DuckDBConnector() as db:
            for account in accounts:
                try:
                    poll_account(db, account["client"], account["type"])
                except Exception as e:
                    print(f"Error polling {account['type']} account: {e}")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    run()
