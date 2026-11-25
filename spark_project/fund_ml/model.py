"""Fit and evaluate a time-split binary classifier for fund purchase.

The delivered target is Spark 2.4.3, so this module uses the 2.4 feature
classes and the Python UDF fallback for probabilities.
"""

import hashlib
import os

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler
from pyspark.sql import functions as F


CATEGORICAL_FEATURES = (
    "customer_type", "risk_level", "investment_capacity", "fund_category",
)
NUMERIC_FEATURES = ("prior_fund_buys_90d", "prior_any_fund_buys_90d")
SPLITS = ("train", "validation", "test")
# Include low cutoffs for datasets with few purchases. The test set plays no role here.
THRESHOLDS = (
    0.0, 0.001, 0.002, 0.005, 0.01, 0.02, 0.03, 0.05, 0.075,
    0.10, 0.15, 0.20, 0.30, 0.50, 0.70, 0.90,
)
PAIR_COLUMNS = ("customer_id", "isin", "as_of_date", "label")


def _count_splits(samples):
    counts = {name: {"rows": 0, "positives": 0} for name in SPLITS}
    for row in samples.groupBy("split", "label").count().collect():
        if row.split not in counts or row.label not in (0.0, 1.0):
            raise ValueError("Expected train, validation or test with binary labels 0 and 1.")
        counts[row.split]["rows"] += row["count"]
        if row.label == 1.0:
            counts[row.split]["positives"] += row["count"]

    for name in SPLITS:
        count = counts[name]
        if not 0 < count["positives"] < count["rows"]:
            raise ValueError("{} must contain both labels ({} positives in {} rows).".format(
                name, count["positives"], count["rows"],
            ))
    return counts


def _pipeline():
    index_columns = [column + "_index" for column in CATEGORICAL_FEATURES]
    encoded_columns = [column + "_encoded" for column in CATEGORICAL_FEATURES]
    indexers = [
        StringIndexer(inputCol=column, outputCol=index, handleInvalid="keep")
        for column, index in zip(CATEGORICAL_FEATURES, index_columns)
    ]
    encoder = OneHotEncoderEstimator(
        inputCols=index_columns, outputCols=encoded_columns, handleInvalid="keep"
    )
    assembler = VectorAssembler(inputCols=encoded_columns + list(NUMERIC_FEATURES), outputCol="features")
    classifier = LogisticRegression(family="binomial", maxIter=50, regParam=0.01)
    return Pipeline(stages=indexers + [encoder, assembler, classifier])


def _scored(model, data):
    # Spark 2.4 has no vector_to_array SQL expression.
    positive_probability = F.udf(lambda value: float(value[1]), "double")("probability")
    return model.transform(data).select(
        *PAIR_COLUMNS, "rawPrediction", positive_probability.alias("probability")
    )


def _ratio(numerator, denominator):
    return float(numerator) / denominator if denominator else 0.0


def _choose_threshold(validation, positives):
    selected_counts = []
    for i, threshold in enumerate(THRESHOLDS):
        predicted_positive = F.col("probability") >= threshold
        selected_counts.extend((
            F.count(F.when(predicted_positive, 1)).alias("p{}".format(i)),
            F.count(F.when(predicted_positive & (F.col("label") == 1), 1)).alias("tp{}".format(i)),
        ))
    row = validation.agg(*selected_counts).first()
    # Break equal-F1 ties toward the higher threshold, which flags fewer pairs.
    return max(
        (_ratio(2 * row["tp{}".format(i)], row["p{}".format(i)] + positives), threshold)
        for i, threshold in enumerate(THRESHOLDS)
    )


def _test_metrics(scored, threshold, counts):
    predicted_positive = F.col("probability") >= threshold
    actual_positive = F.col("label") == 1
    row = scored.agg(
        F.count(F.when(predicted_positive & actual_positive, 1)).alias("tp"),
        F.count(F.when(predicted_positive & ~actual_positive, 1)).alias("fp"),
    ).first()
    tp, fp = row.tp, row.fp
    fn = counts["positives"] - tp
    tn = counts["rows"] - counts["positives"] - fp
    evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction")
    return {
        "test_pr_auc": evaluator.setMetricName("areaUnderPR").evaluate(scored),
        "test_roc_auc": evaluator.setMetricName("areaUnderROC").evaluate(scored),
        # Class prevalence is the reference precision of random rankings.
        "test_baseline_pr_auc": _ratio(counts["positives"], counts["rows"]),
        "test_accuracy": _ratio(tp + tn, counts["rows"]),
        "test_precision": _ratio(tp, tp + fp),
        "test_recall": _ratio(tp, tp + fn),
        "test_f1": _ratio(2 * tp, 2 * tp + fp + fn),
        "test_tp": tp, "test_fp": fp, "test_tn": tn, "test_fn": fn,
    }


def fit_and_score(samples, model_uri):
    """Persist the fitted pipeline and return reloaded test scores and metrics."""
    counts = _count_splits(samples)
    training = samples.where(F.col("split") == "train")
    validation = samples.where(F.col("split") == "validation")
    test = samples.where(F.col("split") == "test")

    fitted = _pipeline().fit(training)
    validation_f1, threshold = _choose_threshold(
        _scored(fitted, validation), counts["validation"]["positives"],
    )
    fitted.write().save(model_uri)
    reloaded = PipelineModel.load(model_uri)

    scored = _scored(reloaded, test).cache()
    try:
        metrics = _test_metrics(scored, threshold, counts["test"])
        metrics["validation_f1"] = validation_f1
        predictions = scored.select(
            *PAIR_COLUMNS,
            "probability",
            (F.col("probability") >= threshold).cast("double").alias("prediction"),
        ).cache()
        predictions.count()
    finally:
        scored.unpersist()
    return predictions, metrics, counts, threshold


def model_digest(model_uri):
    """Return (sha256, bytes) for a local model directory, else (None, None)."""
    path = model_uri
    if path.startswith("file://"):
        path = path[len("file://"):]
    elif "://" in path:
        return None, None
    if not os.path.isdir(path):
        return None, None

    digest = hashlib.sha256()
    total = 0
    for root, directories, files in os.walk(path):
        directories.sort()
        for name in sorted(files):
            full = os.path.join(root, name)
            relative = os.path.relpath(full, path).replace(os.sep, "/")
            digest.update(relative.encode("utf-8"))
            digest.update(b"\0")
            with open(full, "rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(chunk)
                    total += len(chunk)
    return digest.hexdigest(), total
