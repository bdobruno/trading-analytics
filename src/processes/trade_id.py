import polars as pl

ENTRY_INTENTS = ["buy_to_open", "sell_to_open"]
CLOSE_INTENTS = ["buy_to_close", "sell_to_close"]


def assign_trade_ids_to_executions(executions: list[dict]) -> pl.DataFrame:
    """
    Sort executions ASC by filled_at, assign trade_id per symbol via a running
    scoreboard: {symbol: (trade_id, open_qty)}. New entry on a symbol with
    open_qty=0 gets a new trade_id. Closes reduce open_qty.
    """
    rows = sorted(
        [r for r in executions if r.get("position_intent") in ENTRY_INTENTS + CLOSE_INTENTS],
        key=lambda r: r.get("filled_at") or "",
    )

    scoreboard: dict[str, dict] = {}
    trade_id_counter = 1

    for row in rows:
        symbol = row["symbol"]
        qty = float(row["filled_qty"] or 0)
        intent = row["position_intent"]

        if intent in ENTRY_INTENTS:
            if symbol not in scoreboard or scoreboard[symbol]["open_qty"] == 0:
                scoreboard[symbol] = {"trade_id": trade_id_counter, "open_qty": 0}
                trade_id_counter += 1
            scoreboard[symbol]["open_qty"] += qty
        else:
            if symbol in scoreboard:
                scoreboard[symbol]["open_qty"] = max(0, scoreboard[symbol]["open_qty"] - qty)

        row["trade_id"] = scoreboard.get(symbol, {}).get("trade_id")

    df = pl.DataFrame(rows)
    return df.with_columns(pl.col("filled_qty").cast(pl.Float64))


def assign_trade_ids_to_stops(
    stops: list[dict], executions_with_trade_id: pl.DataFrame
) -> pl.DataFrame:
    """
    Sort stops ASC by created_at per symbol. Match each stop to a trade_id by
    consuming entry qty from trades in order until stop qty is accounted for.
    """
    if not stops:
        return pl.DataFrame()

    # build ordered queue of (trade_id, remaining_qty) per symbol
    entries = (
        executions_with_trade_id.filter(
            pl.col("position_intent").is_in(ENTRY_INTENTS)
        )
        .group_by(["symbol", "trade_id"])
        .agg(pl.col("filled_qty").sum().alias("entry_qty"))
        .sort(["symbol", "trade_id"])
    )

    queue: dict[str, list[dict]] = {}
    for row in entries.iter_rows(named=True):
        queue.setdefault(row["symbol"], []).append(
            {"trade_id": row["trade_id"], "remaining": float(row["entry_qty"])}
        )

    rows = sorted(stops, key=lambda r: r.get("created_at") or "")

    for row in rows:
        symbol = row["symbol"]
        qty = float(row["qty"] or 0)
        bucket = queue.get(symbol, [])

        if not bucket:
            row["trade_id"] = None
            continue

        row["trade_id"] = bucket[0]["trade_id"]
        bucket[0]["remaining"] -= qty
        if bucket[0]["remaining"] <= 0:
            bucket.pop(0)

    return pl.DataFrame(rows)
