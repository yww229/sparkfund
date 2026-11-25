"""Summarize each customer's subscriptions to each fund."""


def build(trades, customers, funds, strategy):
    from pyspark.sql import functions as F

    with_dimensions = (
        trades.join(customers.select("customer_id", "segment_code"), "customer_id")
        .join(funds.select("fund_id", "fund_type"), "fund_id")
    )
    return with_dimensions.groupBy("customer_id", "fund_id", "segment_code", "fund_type").agg(
        F.count("*").alias("subscription_count"),
        F.sum("amount").cast("decimal(22,2)").alias("subscription_amount"),
        F.max("confirm_date").alias("last_confirm_date"),
    )
