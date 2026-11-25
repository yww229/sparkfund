-- Run in spark_project_demo after setting @demo_run_id.
SELECT COUNT(*), MAX(r.test_rows), MAX(r.threshold)
INTO @manifests, @expected_rows, @threshold
FROM fund_ml_run r JOIN far_dataset d ON d.data_version = r.data_version
WHERE r.run_id = @demo_run_id AND r.train_rows > 0 AND r.validation_rows > 0
  AND r.test_rows > 0 AND r.threshold BETWEEN 0 AND 1
  AND LENGTH(r.model_uri) > 0 AND LENGTH(r.spark_app_id) > 0 AND LENGTH(r.code_version) > 0;

-- Recompute classification, confusion counts and future buy labels from MySQL.
SELECT COUNT(*), SUM(label = 1 AND prediction = 1), SUM(label = 0 AND prediction = 1),
       SUM(label = 0 AND prediction = 0), SUM(label = 1 AND prediction = 0),
       SUM(probability NOT BETWEEN 0 AND 1 OR prediction NOT IN (0, 1) OR label NOT IN (0, 1)
           OR prediction <> (probability >= @threshold)),
       SUM(label <> observed_label)
INTO @observed_rows, @tp, @fp, @tn, @fn, @bad_prediction, @bad_label
FROM (
    SELECT p.as_of_date, p.label, p.probability, p.prediction,
           EXISTS (
               SELECT 1 FROM far_trade t
               WHERE t.customer_id = p.customer_id AND t.isin = p.isin AND t.trade_type = 'Buy'
                 AND t.trade_date > p.as_of_date
                 AND t.trade_date <= DATE_ADD(p.as_of_date, INTERVAL 30 DAY)
                 AND EXISTS (SELECT 1 FROM far_asset a WHERE a.isin = t.isin
                             AND t.trade_date > a.first_price_date AND t.trade_date <= a.last_price_date)
           ) AS observed_label
    FROM fund_ml_prediction p WHERE p.run_id = @demo_run_id
) AS audited;

SELECT COUNT(*) INTO @metric_rows
FROM fund_ml_metric WHERE run_id = @demo_run_id;

SELECT COUNT(*) INTO @matched_metrics
FROM fund_ml_metric m
WHERE m.run_id = @demo_run_id AND (
    (m.metric_name = 'test_tp' AND m.metric_value = @tp)
    OR (m.metric_name = 'test_fp' AND m.metric_value = @fp)
    OR (m.metric_name = 'test_tn' AND m.metric_value = @tn)
    OR (m.metric_name = 'test_fn' AND m.metric_value = @fn)
    OR (m.metric_name = 'test_accuracy' AND ABS(m.metric_value - (@tp + @tn) * 1.0E0 / @observed_rows) < 0.00000001)
    OR (m.metric_name = 'test_precision' AND ABS(m.metric_value - IF(@tp + @fp = 0, 0, @tp * 1.0E0 / (@tp + @fp))) < 0.00000001)
    OR (m.metric_name = 'test_recall' AND ABS(m.metric_value - IF(@tp + @fn = 0, 0, @tp * 1.0E0 / (@tp + @fn))) < 0.00000001)
    OR (m.metric_name = 'test_f1' AND ABS(m.metric_value - IF(2 * @tp + @fp + @fn = 0, 0, 2 * @tp * 1.0E0 / (2 * @tp + @fp + @fn))) < 0.00000001)
    OR (m.metric_name = 'test_baseline_pr_auc' AND ABS(m.metric_value - (@tp + @fn) * 1.0E0 / @observed_rows) < 0.00000001)
    OR (m.metric_name IN ('test_pr_auc', 'test_roc_auc', 'validation_f1') AND m.metric_value BETWEEN 0 AND 1)
);

SELECT 'held_out_rows', @observed_rows, 'label_errors', @bad_label, 'classification_errors', @bad_prediction;
SELECT 'tp', @tp, 'fp', @fp, 'tn', @tn, 'fn', @fn, 'matched_metrics', @matched_metrics;
SELECT IF(@manifests = 1 AND @observed_rows = @expected_rows AND @bad_prediction = 0 AND @bad_label = 0
          AND @tp + @fn > 0 AND @tn + @fp > 0
          AND @matched_metrics = @metric_rows AND @metric_rows >= 12, 'PASS', 'FAIL');
