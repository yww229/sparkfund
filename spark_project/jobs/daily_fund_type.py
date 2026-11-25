"""Summarize subscriptions by confirmation date and fund category."""


def lookup_input(trades, strategy):
    """Return the trade-side rows that enter the fund dimension join."""
    from pyspark.sql import functions as F

    if strategy == "preagg":
        # Multiple transactions for one fund/day can share a single dimension lookup.
        return trades.groupBy("confirm_date", "fund_id").agg(
            F.count("*").alias("subscription_count"),
            F.sum("amount").alias("subscription_amount"),
        )
    if strategy == "direct":
        return trades.select(
            "confirm_date", "fund_id",
            F.lit(1).cast("bigint").alias("subscription_count"),
            F.col("amount").alias("subscription_amount"),
        )
    raise ValueError("Unsupported fund lookup strategy: {0}".format(strategy))


def build(trades, customers, funds, strategy):
    from pyspark.sql import functions as F

    with_category = lookup_input(trades, strategy).join(funds.select("fund_id", "fund_type"), "fund_id")
    # Both paths sum the same decimal amounts and count transactions, not lookup groups.
    return with_category.groupBy("confirm_date", "fund_type").agg(
        F.sum("subscription_count").alias("subscription_count"),
        F.sum("subscription_amount").cast("decimal(22,2)").alias("subscription_amount"),
    )
