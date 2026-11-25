"""Build date-stamped customer/fund examples from observed FAR-Trans trades."""

import os

from pyspark.sql import Window
from pyspark.sql import functions as F


OBSERVATIONS = (
    ("2020-06-30", "train"),
    ("2020-09-30", "train"),
    ("2020-12-31", "train"),
    ("2021-03-31", "train"),
    ("2021-06-30", "train"),
    ("2021-09-30", "train"),
    ("2021-12-31", "validation"),
    ("2022-03-31", "validation"),
    ("2022-06-30", "test"),
    ("2022-09-30", "test"),
)
LABEL_DAYS = 30
HISTORY_DAYS = 90
ACTIVE_DAYS = 365
MAX_FUNDS = 10
MAX_CUSTOMERS_PER_DATE = 3000
# Purchase pairs are rare (~0.08%). Keep every purchase and this many non-purchases
# per observation date so the classifier and its threshold see a usable positive rate.
NEGATIVES_PER_POSITIVE = int(os.environ.get("DEMO_NEGATIVES_PER_POSITIVE", "20"))
if NEGATIVES_PER_POSITIVE < 1:
    raise ValueError("DEMO_NEGATIVES_PER_POSITIVE must be at least 1.")


def sample_signature():
    """Describe the sampling window so two runs can be compared later."""
    return "obs={0};label={1};history={2};active={3};funds={4};customers={5};neg={6}".format(
        ",".join(day for day, _ in OBSERVATIONS),
        LABEL_DAYS,
        HISTORY_DAYS,
        ACTIVE_DAYS,
        MAX_FUNDS,
        MAX_CUSTOMERS_PER_DATE,
        NEGATIVES_PER_POSITIVE,
    )


def _observation_dates(spark):
    # Generate constants within Spark instead of dispatching Python work to executors.
    observations = [
        F.struct(F.lit(day).cast("date").alias("as_of_date"), F.lit(split).alias("split"))
        for day, split in OBSERVATIONS
    ]
    return spark.range(1).select(
        F.explode(F.array(*observations)).alias("observation")
    ).select("observation.*")


def _as_of_snapshots(rows, dates, key, effective_date, fields):
    candidates = rows.crossJoin(F.broadcast(dates.select("as_of_date"))).where(
        F.col(effective_date) <= F.col("as_of_date")
    )
    ordering = [F.col(effective_date).desc()] + [F.col(name).asc() for name in fields]
    latest = Window.partitionBy("as_of_date", key).orderBy(*ordering)
    return candidates.withColumn("snapshot_number", F.row_number().over(latest)).where(
        F.col("snapshot_number") == 1
    ).select("as_of_date", key, *fields)


def _eligible_trades(trades, price_ranges):
    # The release reconstructs opening buys and end-of-series sales on price boundaries.
    # Exclude those rows conservatively, even when a real trade may share the same date.
    usable_buy = (F.col("trade_type") == "Buy") & (F.col("trade_date") > F.col("first_price_date"))
    usable_sell = (F.col("trade_type") == "Sell") & (F.col("trade_date") < F.col("last_price_date"))
    return trades.join(F.broadcast(price_ranges), "isin").where(
        F.col("trade_date").between(F.col("first_price_date"), F.col("last_price_date"))
        & (usable_buy | usable_sell)
    ).select("customer_id", "isin", "trade_date", "trade_type")


def _fund_pool(fund_snapshots, buys):
    initial_day = F.lit(OBSERVATIONS[0][0]).cast("date")
    eligible_at_start = fund_snapshots.where(F.col("as_of_date") == initial_day).select("isin")
    return buys.where(F.col("trade_date") <= initial_day).join(
        eligible_at_start, "isin", "left_semi"
    ).groupBy("isin").count().orderBy(F.col("count").desc(), "isin").limit(MAX_FUNDS).select("isin")


def _active_customers(trades, dates, profiles):
    activity = trades.crossJoin(F.broadcast(dates.select("as_of_date"))).where(
        (F.col("trade_date") > F.date_sub("as_of_date", ACTIVE_DAYS))
        & (F.col("trade_date") <= F.col("as_of_date"))
    )
    recent_buy = (F.col("trade_type") == "Buy") & (
        F.col("trade_date") > F.date_sub("as_of_date", HISTORY_DAYS)
    )
    active = activity.groupBy("as_of_date", "customer_id").agg(
        F.count(F.when(recent_buy, 1)).cast("double").alias("prior_any_fund_buys_90d")
    ).join(profiles, ["as_of_date", "customer_id"])
    # Stable subsampling after eligibility, using only the anonymized identifier.
    selection = Window.partitionBy("as_of_date").orderBy(F.hash("customer_id"), "customer_id")
    return active.withColumn("sample_number", F.row_number().over(selection)).where(
        F.col("sample_number") <= MAX_CUSTOMERS_PER_DATE
    ).drop("sample_number")


def _fund_activity(buys, pool, dates):
    dated_buys = buys.join(F.broadcast(pool), "isin", "left_semi").crossJoin(
        F.broadcast(dates.select("as_of_date"))
    ).where(
        (F.col("trade_date") > F.date_sub("as_of_date", HISTORY_DAYS))
        & (F.col("trade_date") <= F.date_add("as_of_date", LABEL_DAYS))
    )
    past = F.col("trade_date") <= F.col("as_of_date")
    return dated_buys.groupBy("as_of_date", "customer_id", "isin").agg(
        F.count(F.when(past, 1)).cast("double").alias("prior_fund_buys_90d"),
        F.max(F.when(past, 0.0).otherwise(1.0)).alias("label"),
    )


def _balance_negatives(samples):
    """Keep all purchases plus a fixed number of deterministic non-purchases per date."""
    positives = samples.where(F.col("label") == 1)
    negatives = samples.where(F.col("label") == 0)
    allowance = positives.groupBy("as_of_date", "split").agg(
        (F.count("*") * NEGATIVES_PER_POSITIVE).alias("allowance")
    )
    ordering = Window.partitionBy("as_of_date", "split").orderBy(
        F.hash("customer_id", "isin"), "customer_id", "isin"
    )
    kept_negatives = negatives.join(F.broadcast(allowance), ["as_of_date", "split"]).withColumn(
        "rank", F.row_number().over(ordering)
    ).where(F.col("rank") <= F.col("allowance")).drop("rank", "allowance")
    return positives.unionByName(kept_negatives)


def build_samples(spark, customers, assets, trades):
    """Label each eligible customer/fund at T by recorded buys on days T+1 to T+30."""
    dates = _observation_dates(spark)
    customer_fields = ("customer_type", "risk_level", "investment_capacity")
    profiles = _as_of_snapshots(customers, dates, "customer_id", "profile_date", customer_fields)
    funds = _as_of_snapshots(assets, dates, "isin", "asset_date", ("fund_category",))
    price_ranges = assets.groupBy("isin").agg(
        F.min("first_price_date").alias("first_price_date"),
        F.max("last_price_date").alias("last_price_date"),
    )
    # Fund price coverage is tested through the observation date, prior to the label window.
    covered_funds = funds.join(F.broadcast(price_ranges), "isin").where(
        F.col("as_of_date").between(F.col("first_price_date"), F.col("last_price_date"))
    ).select("as_of_date", "isin", "fund_category")

    eligible_trades = _eligible_trades(trades, price_ranges)
    buys = eligible_trades.where(F.col("trade_type") == "Buy")
    pool = _fund_pool(covered_funds, buys)
    active = _active_customers(eligible_trades, dates, profiles)
    targets = active.join(F.broadcast(covered_funds.join(pool, "isin")), "as_of_date")
    per_fund = _fund_activity(buys, pool, dates)
    samples = targets.join(per_fund, ["as_of_date", "customer_id", "isin"], "left").join(
        F.broadcast(dates), "as_of_date"
    ).fillna({
        "label": 0.0,
        "prior_fund_buys_90d": 0.0,
        "customer_type": "Unknown",
        "risk_level": "Unknown",
        "investment_capacity": "Unknown",
        "fund_category": "Unknown",
    }).select(
        "customer_id", "isin", "as_of_date", "split", "label",
        "customer_type", "risk_level", "investment_capacity", "fund_category",
        "prior_fund_buys_90d", "prior_any_fund_buys_90d",
    )
    return _balance_negatives(samples)
