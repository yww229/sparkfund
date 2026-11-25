-- Remove every row written by one run. Run after setting @demo_run_id.
DELETE FROM fund_ml_prediction WHERE run_id = @demo_run_id;
DELETE FROM fund_ml_metric WHERE run_id = @demo_run_id;
DELETE FROM demo_run WHERE run_id = @demo_run_id;
DELETE FROM fund_ml_run WHERE run_id = @demo_run_id;
DELETE FROM customer_fund_summary WHERE run_id = @demo_run_id;
DELETE FROM daily_fund_type_summary WHERE run_id = @demo_run_id;

SELECT 'demo_run' AS table_name, COUNT(*) AS remaining FROM demo_run WHERE run_id = @demo_run_id
UNION ALL SELECT 'customer_fund_summary', COUNT(*) FROM customer_fund_summary WHERE run_id = @demo_run_id
UNION ALL SELECT 'daily_fund_type_summary', COUNT(*) FROM daily_fund_type_summary WHERE run_id = @demo_run_id
UNION ALL SELECT 'fund_ml_run', COUNT(*) FROM fund_ml_run WHERE run_id = @demo_run_id
UNION ALL SELECT 'fund_ml_metric', COUNT(*) FROM fund_ml_metric WHERE run_id = @demo_run_id
UNION ALL SELECT 'fund_ml_prediction', COUNT(*) FROM fund_ml_prediction WHERE run_id = @demo_run_id;
