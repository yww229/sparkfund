-- Compare two completed daily jobs in spark_project_demo. Set @demo_direct_run
-- and @demo_preagg_run in this session. Requires only SELECT access.
SELECT COUNT(*) INTO @direct_rows
FROM daily_fund_type_summary
WHERE run_id = @demo_direct_run;

SELECT COUNT(*) INTO @preagg_rows
FROM daily_fund_type_summary
WHERE run_id = @demo_preagg_run;

-- The primary key guarantees one row per date and fund type for each run.
SELECT COUNT(*) INTO @matching_rows
FROM daily_fund_type_summary AS direct_result
INNER JOIN daily_fund_type_summary AS preagg_result
    ON preagg_result.run_id = @demo_preagg_run
   AND preagg_result.confirm_date = direct_result.confirm_date
   AND preagg_result.fund_type = direct_result.fund_type
   AND preagg_result.subscription_count = direct_result.subscription_count
   AND preagg_result.subscription_amount = direct_result.subscription_amount
WHERE direct_result.run_id = @demo_direct_run;

SELECT COUNT(*) INTO @matching_manifest
FROM demo_run AS direct_run
INNER JOIN demo_run AS preagg_run
    ON preagg_run.run_id = @demo_preagg_run
   AND preagg_run.job_name = 'daily_fund_type'
   AND preagg_run.strategy = 'preagg'
   AND preagg_run.date_from = direct_run.date_from
   AND preagg_run.date_to = direct_run.date_to
   AND preagg_run.result_rows = @preagg_rows
WHERE direct_run.run_id = @demo_direct_run
  AND direct_run.job_name = 'daily_fund_type'
  AND direct_run.strategy = 'direct'
  AND direct_run.result_rows = @direct_rows;

SELECT 'direct_rows' AS direct_rows, 'preagg_rows' AS preagg_rows,
       'matched_rows' AS matched_rows, 'matched_manifests' AS matched_manifests
UNION ALL
SELECT @direct_rows, @preagg_rows, @matching_rows, @matching_manifest;

SELECT CASE
    WHEN @demo_direct_run <> @demo_preagg_run
     AND @matching_manifest = 1
     AND @direct_rows = @preagg_rows
     AND @direct_rows = @matching_rows
    THEN 'PASS'
    ELSE 'FAIL'
END AS status;
