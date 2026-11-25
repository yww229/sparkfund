-- Run inside spark_project_demo with @demo_run_id, @demo_job, @demo_from,
-- @demo_to and @demo_strategy set in this session. Requires only SELECT access.
SET @from_date = CAST(@demo_from AS DATE), @to_date = CAST(@demo_to AS DATE);

-- Recompute customer-fund totals directly from the original transactions.
SELECT COUNT(*), COUNT(actual.run_id) INTO @expected_customer, @matched_customer
FROM (
    SELECT trade.customer_id,
           trade.fund_id,
           customer.segment_code,
           fund.fund_type,
           COUNT(*) AS subscription_count,
           MAX(trade.confirm_date) AS last_confirm_date,
           SUM(trade.amount) AS subscription_amount
    FROM demo_fund_trade AS trade
    INNER JOIN demo_customer AS customer ON customer.customer_id = trade.customer_id
    INNER JOIN demo_fund AS fund ON fund.fund_id = trade.fund_id
    WHERE @demo_job = 'customer_fund'
      AND trade.trade_type = 'SUBSCRIBE'
      AND trade.trade_status = 'CONFIRMED'
      AND trade.amount > 0
      AND trade.confirm_date BETWEEN @from_date AND @to_date
    GROUP BY trade.customer_id, trade.fund_id, customer.segment_code, fund.fund_type
) AS expected
LEFT JOIN customer_fund_summary AS actual
    ON actual.run_id = @demo_run_id
   AND actual.customer_id = expected.customer_id
   AND actual.fund_id = expected.fund_id
   AND actual.segment_code = expected.segment_code
   AND actual.fund_type = expected.fund_type
   AND actual.subscription_count = expected.subscription_count
   AND actual.last_confirm_date = expected.last_confirm_date
   AND actual.subscription_amount = expected.subscription_amount;

-- Recompute daily fund-type totals using a direct join on source detail.
SELECT COUNT(*), COUNT(actual.run_id) INTO @expected_daily, @matched_daily
FROM (
    SELECT trade.confirm_date,
           fund.fund_type,
           COUNT(*) AS subscription_count,
           SUM(trade.amount) AS subscription_amount
    FROM demo_fund_trade AS trade
    INNER JOIN demo_customer AS customer ON customer.customer_id = trade.customer_id
    INNER JOIN demo_fund AS fund ON fund.fund_id = trade.fund_id
    WHERE @demo_job = 'daily_fund_type'
      AND trade.trade_type = 'SUBSCRIBE'
      AND trade.trade_status = 'CONFIRMED'
      AND trade.amount > 0
      AND trade.confirm_date BETWEEN @from_date AND @to_date
    GROUP BY trade.confirm_date, fund.fund_type
) AS expected
LEFT JOIN daily_fund_type_summary AS actual
    ON actual.run_id = @demo_run_id
   AND actual.confirm_date = expected.confirm_date
   AND actual.fund_type = expected.fund_type
   AND actual.subscription_count = expected.subscription_count
   AND actual.subscription_amount = expected.subscription_amount;

SELECT COUNT(*) INTO @stored_customer
FROM customer_fund_summary
WHERE run_id = @demo_run_id;

SELECT COUNT(*) INTO @stored_daily
FROM daily_fund_type_summary
WHERE run_id = @demo_run_id;

SELECT COUNT(*),
       COALESCE(SUM(job_name = @demo_job
                AND strategy = @demo_strategy
                AND date_from = @from_date AND date_to = @to_date
                AND result_rows = @expected_customer + @expected_daily
                AND LENGTH(TRIM(spark_app_id)) > 0
                AND LENGTH(TRIM(code_version)) > 0), 0)
INTO @stored_run, @matched_run
FROM demo_run
WHERE run_id = @demo_run_id;

SELECT 'check_name' AS check_name, 'expected_rows' AS expected_rows,
       'stored_rows' AS stored_rows, 'matched_rows' AS matched_rows
UNION ALL
SELECT 'customer_fund_summary', @expected_customer, @stored_customer, @matched_customer
UNION ALL
SELECT 'daily_fund_type_summary', @expected_daily, @stored_daily, @matched_daily
UNION ALL
SELECT 'demo_run', 1, @stored_run, @matched_run;

SELECT CASE
    WHEN @from_date <= @to_date
     AND ((@demo_job = 'customer_fund' AND @demo_strategy = 'direct')
          OR (@demo_job = 'daily_fund_type' AND @demo_strategy IN ('direct', 'preagg')))
     AND @expected_customer = @stored_customer
     AND @expected_customer = @matched_customer
     AND @expected_daily = @stored_daily
     AND @expected_daily = @matched_daily
     AND (@expected_customer + @expected_daily) > 0
     AND @matched_run = 1
    THEN 'PASS'
    ELSE 'FAIL'
END AS status;
