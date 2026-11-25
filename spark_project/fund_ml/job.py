"""Train the fund purchase model, persist scores and record the run."""

from pyspark.sql import functions as F

from spark_project import settings, tables
from spark_project.fund_ml.model import fit_and_score, model_digest
from spark_project.fund_ml.samples import build_samples, sample_signature


def run_fund_purchase(spark, store, args):
    store.require_unused_run((tables.ML_RUN, tables.ML_METRIC, tables.ML_PREDICTION), args.run_id)

    source = store.read(tables.FAR_DATASET, columns=("data_version", "coverage_end")).take(2)
    if len(source) != 1 or str(source[0].coverage_end) < "2022-10-30":
        raise RuntimeError("Load the complete, pinned FAR-Trans release before training.")
    data_version = source[0].data_version

    root = settings.model_root(required=True)
    model_uri = root + "/" + args.run_id
    if len(model_uri) > 512:
        raise RuntimeError("The model URI exceeds the 512-character manifest column.")

    samples = build_samples(
        spark,
        store.read(
            tables.FAR_CUSTOMER,
            columns=("customer_id", "profile_date", "customer_type", "risk_level", "investment_capacity"),
        ),
        store.read(
            tables.FAR_ASSET,
            columns=("isin", "asset_date", "fund_category", "first_price_date", "last_price_date"),
        ),
        store.read(
            tables.FAR_TRADE,
            columns=("customer_id", "isin", "trade_date", "trade_type"),
        ),
    ).cache()

    predictions = None
    try:
        predictions, metrics, counts, threshold = fit_and_score(samples, model_uri)
        try:
            stored = store.append_and_count(tables.ML_PREDICTION, predictions, args.run_id)
            if stored != counts["test"]["rows"]:
                raise RuntimeError(
                    "Prediction readback mismatch: {0} vs {1}.".format(
                        stored, counts["test"]["rows"]
                    )
                )
        finally:
            predictions.unpersist()
    finally:
        samples.unpersist()

    metric_frame = spark.createDataFrame(
        [(name, float(value)) for name, value in sorted(metrics.items())],
        "metric_name string, metric_value double",
    )
    if store.append_and_count(tables.ML_METRIC, metric_frame, args.run_id) != len(metrics):
        raise RuntimeError("Metric readback mismatch.")

    checksum, model_bytes = model_digest(model_uri)
    manifest = spark.range(1).select(
        F.lit(args.code_version).alias("code_version"),
        F.lit(spark.sparkContext.applicationId).alias("spark_app_id"),
        F.lit(data_version).alias("data_version"),
        F.lit(model_uri).alias("model_uri"),
        F.lit(counts["train"]["rows"]).cast("long").alias("train_rows"),
        F.lit(counts["validation"]["rows"]).cast("long").alias("validation_rows"),
        F.lit(counts["test"]["rows"]).cast("long").alias("test_rows"),
        F.lit(threshold).cast("double").alias("threshold"),
        F.lit(sample_signature()).alias("sample_signature"),
        F.lit(checksum).cast("string").alias("model_checksum"),
        F.lit(model_bytes).cast("long").alias("model_bytes"),
        F.lit(args.log_path).alias("log_path"),
    )
    store.register_run(tables.ML_RUN, args.run_id, manifest)

    print("MODEL_URI={0}".format(model_uri), flush=True)
    print("DATA_VERSION={0}".format(data_version), flush=True)
    for name in sorted(counts):
        print("ML_SPLIT {} rows={} positives={}".format(
            name, counts[name]["rows"], counts[name]["positives"]
        ), flush=True)
    print("ML_THRESHOLD={:.6f}".format(threshold), flush=True)
    for name, value in sorted(metrics.items()):
        print("ML_METRIC {}={:.8f}".format(name, float(value)), flush=True)
    return stored
