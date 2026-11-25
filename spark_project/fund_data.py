"""Prepare the same trade input for both fund reports."""


def scope_sql(date_from, date_to):
    """Return the WHERE clause pushed into MySQL before Spark reads trades."""
    return (
        "trade_type = 'SUBSCRIBE' AND trade_status = 'CONFIRMED' AND amount > 0 "
        "AND confirm_date BETWEEN '{0}' AND '{1}'"
    ).format(date_from, date_to)


def eligible_trades(trades, date_from, date_to):
    """Keep positive confirmed subscriptions in the inclusive date window."""
    from pyspark.sql import functions as F

    start = F.lit(date_from).cast("date")
    end = F.lit(date_to).cast("date")
    return trades.where(
        (F.col("trade_type") == "SUBSCRIBE")
        & (F.col("trade_status") == "CONFIRMED")
        & (F.col("amount") > 0)
        & F.col("confirm_date").between(start, end)
    ).select("customer_id", "fund_id", "confirm_date", "amount")
