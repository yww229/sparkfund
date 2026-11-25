-- One run's manifest, result counts, metrics and log path.
SELECT 'demo_run' AS section, run_id, job_name, strategy, date_from, date_to,
       result_rows, code_version, log_path
FROM demo_run WHERE run_id = @demo_run_id;

SELECT 'result_rows' AS section, 'customer_fund_summary' AS name, COUNT(*) AS value
FROM customer_fund_summary WHERE run_id = @demo_run_id
UNION ALL
SELECT 'result_rows', 'daily_fund_type_summary', COUNT(*)
FROM daily_fund_type_summary WHERE run_id = @demo_run_id;

SELECT 'fund_ml_run' AS section, run_id, data_version, train_rows, validation_rows,
       test_rows, threshold, code_version, sample_signature, model_checksum,
       model_bytes, log_path
FROM fund_ml_run WHERE run_id = @demo_run_id;

SELECT 'metric' AS section, metric_name, metric_value
FROM fund_ml_metric WHERE run_id = @demo_run_id ORDER BY metric_name;
