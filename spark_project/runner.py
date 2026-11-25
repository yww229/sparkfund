"""Execute a registered ETL job and record its manifest."""

from pyspark.sql import functions as F

from spark_project import tables
from spark_project.fund_data import eligible_trades, scope_sql


def run_job(spark, store, args, spec):
    store.require_unused_run((tables.DEMO_RUN,) + spec.run_tables, args.run_id)

    trades = eligible_trades(
        store.read(
            tables.DEMO_TRADE,
            columns=("customer_id", "fund_id", "trade_type", "trade_status", "confirm_date", "amount"),
            where=scope_sql(args.date_from, args.date_to),
        ),
        args.date_from,
        args.date_to,
    )
    customers = store.read(tables.DEMO_CUSTOMER, columns=("customer_id", "segment_code"))
    funds = store.read(tables.DEMO_FUND, columns=("fund_id", "fund_type"))

    output_frame = spec.build(trades, customers, funds, args.strategy).cache()
    try:
        if args.plan:
            output_frame.explain(True)
            if spec.lookup_input:
                print(
                    "LOOKUP_INPUT_ROWS={0}".format(
                        spec.lookup_input(trades, args.strategy).count()
                    ),
                    flush=True,
                )
        expected = output_frame.count()
        stored = store.append_and_count(spec.run_tables[0], output_frame, args.run_id)
        if stored != expected:
            raise RuntimeError(
                "Wrote {0} rows but read back {1}; the run is not trustworthy.".format(
                    expected, stored
                )
            )
    finally:
        output_frame.unpersist()

    receipt = spark.range(1).select(
        F.lit(args.job).alias("job_name"),
        F.lit(args.strategy).alias("strategy"),
        F.lit(args.date_from).cast("date").alias("date_from"),
        F.lit(args.date_to).cast("date").alias("date_to"),
        F.lit(args.code_version).alias("code_version"),
        F.lit(spark.sparkContext.applicationId).alias("spark_app_id"),
        F.lit(stored).cast("long").alias("result_rows"),
        F.lit(args.log_path).alias("log_path"),
        F.current_timestamp().alias("completed_at"),
    )
    store.register_run(tables.DEMO_RUN, args.run_id, receipt)
    return stored
