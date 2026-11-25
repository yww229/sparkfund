-- Initialize the synthetic demo tables.
-- Run with: mysql --database=spark_project_demo < sql/setup.sql

CREATE TABLE IF NOT EXISTS demo_customer (
    customer_id VARCHAR(16) NOT NULL,
    customer_name VARCHAR(64) NOT NULL,
    segment_code VARCHAR(16) NOT NULL,
    PRIMARY KEY (customer_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS demo_fund (
    fund_id VARCHAR(16) NOT NULL,
    fund_name VARCHAR(64) NOT NULL,
    fund_type VARCHAR(16) NOT NULL,
    PRIMARY KEY (fund_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS demo_fund_trade (
    trade_id VARCHAR(32) NOT NULL,
    customer_id VARCHAR(16) NOT NULL,
    fund_id VARCHAR(16) NOT NULL,
    confirm_date DATE NOT NULL,
    trade_type VARCHAR(16) NOT NULL,
    trade_status VARCHAR(16) NOT NULL,
    amount DECIMAL(16,2) NOT NULL,
    PRIMARY KEY (trade_id),
    KEY ix_trade_scope (trade_type, trade_status, confirm_date),
    CONSTRAINT fk_trade_customer FOREIGN KEY (customer_id) REFERENCES demo_customer (customer_id),
    CONSTRAINT fk_trade_fund FOREIGN KEY (fund_id) REFERENCES demo_fund (fund_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS customer_fund_summary (
    run_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    customer_id VARCHAR(16) NOT NULL,
    fund_id VARCHAR(16) NOT NULL,
    segment_code VARCHAR(16) NOT NULL,
    fund_type VARCHAR(16) NOT NULL,
    subscription_count BIGINT NOT NULL,
    subscription_amount DECIMAL(22,2) NOT NULL,
    last_confirm_date DATE NULL,
    PRIMARY KEY (run_id, customer_id, fund_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS daily_fund_type_summary (
    run_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    confirm_date DATE NOT NULL,
    fund_type VARCHAR(16) NOT NULL,
    subscription_count BIGINT NOT NULL,
    subscription_amount DECIMAL(22,2) NOT NULL,
    PRIMARY KEY (run_id, confirm_date, fund_type)
) ENGINE=InnoDB;

-- A run is registered only after its job writes its result rows.
CREATE TABLE IF NOT EXISTS demo_run (
    run_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    job_name VARCHAR(32) NOT NULL,
    strategy VARCHAR(16) NOT NULL,
    date_from DATE NOT NULL,
    date_to DATE NOT NULL,
    result_rows BIGINT NOT NULL,
    spark_app_id VARCHAR(128) NOT NULL,
    code_version VARCHAR(64) NOT NULL,
    log_path VARCHAR(255) NULL,
    completed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (run_id)
) ENGINE=InnoDB;

-- Existing demo databases predate log_path; add it once without failing a rerun.
SET @ddl = (
    SELECT IF(COUNT(*) = 0,
              'ALTER TABLE demo_run ADD COLUMN log_path VARCHAR(255) NULL',
              'SET @demo_noop = 1')
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'demo_run' AND column_name = 'log_path'
);
PREPARE demo_ddl FROM @ddl;
EXECUTE demo_ddl;
DEALLOCATE PREPARE demo_ddl;

-- Use stable identifiers. Re-running setup preserves existing rows, including results.
START TRANSACTION;

INSERT INTO demo_customer (customer_id, customer_name, segment_code)
VALUES ('C001', 'Demo customer 01', 'RETAIL'),
       ('C002', 'Demo customer 02', 'RETAIL'),
       ('C003', 'Demo customer 03', 'RETAIL'),
       ('C004', 'Demo customer 04', 'RETAIL'),
       ('C005', 'Demo customer 05', 'RETAIL'),
       ('C006', 'Demo customer 06', 'AFFLUENT'),
       ('C007', 'Demo customer 07', 'AFFLUENT'),
       ('C008', 'Demo customer 08', 'AFFLUENT'),
       ('C009', 'Demo customer 09', 'INSTITUTION'),
       ('C010', 'Demo customer 10', 'INSTITUTION')
ON DUPLICATE KEY UPDATE customer_id = demo_customer.customer_id;

INSERT INTO demo_fund (fund_id, fund_name, fund_type)
VALUES ('F001', 'Demo equity fund A', 'EQUITY'),
       ('F002', 'Demo equity fund B', 'EQUITY'),
       ('F003', 'Demo bond fund A', 'BOND'),
       ('F004', 'Demo bond fund B', 'BOND'),
       ('F005', 'Demo mixed fund A', 'MIXED'),
       ('F006', 'Demo mixed fund B', 'MIXED')
ON DUPLICATE KEY UPDATE fund_id = demo_fund.fund_id;

-- In January the hand-check pair C001/F001 has two eligible trades worth 300.00.
INSERT INTO demo_fund_trade (
    trade_id, customer_id, fund_id, confirm_date, trade_type, trade_status, amount
)
VALUES ('S001', 'C001', 'F001', '2025-01-06', 'SUBSCRIBE', 'CONFIRMED', 100.00),
       ('S002', 'C001', 'F001', '2025-01-08', 'SUBSCRIBE', 'CONFIRMED', 200.00),
       ('S003', 'C001', 'F001', '2025-01-08', 'SUBSCRIBE', 'FAILED', 500.00),
       ('S004', 'C001', 'F001', '2025-01-09', 'REDEEM', 'CONFIRMED', 300.00),
       ('S005', 'C001', 'F001', '2025-01-10', 'SUBSCRIBE', 'PENDING', 120.00),
       ('S006', 'C001', 'F001', '2025-02-03', 'SUBSCRIBE', 'CONFIRMED', 400.00)
ON DUPLICATE KEY UPDATE trade_id = demo_fund_trade.trade_id;

-- 1,534 generated rows span Jan 6-31, with many trades sharing a date and fund.
-- Synthetic dates intentionally include weekends so range checks are reproducible.
INSERT INTO demo_fund_trade (
    trade_id, customer_id, fund_id, confirm_date, trade_type, trade_status, amount
)
SELECT CONCAT('G-', seed.customer_id, '-', seed.fund_id, '-', DATE_FORMAT(seed.trade_date, '%Y%m%d')),
       seed.customer_id,
       seed.fund_id,
       seed.trade_date,
       CASE WHEN MOD(seed.variant, 7) = 0 THEN 'REDEEM' ELSE 'SUBSCRIBE' END,
       CASE
           WHEN MOD(seed.variant, 13) = 0 THEN 'FAILED'
           WHEN MOD(seed.variant, 11) = 0 THEN 'PENDING'
           ELSE 'CONFIRMED'
       END,
       CASE
           WHEN MOD(seed.variant, 29) = 0 THEN 0.00
           WHEN MOD(seed.variant, 37) = 0 THEN -10.00
           ELSE 25.00 + 41 * seed.customer_no + 13 * seed.fund_no
                + 9 * seed.day_no + MOD(seed.variant, 100) * 0.01
       END
FROM (
    SELECT customer.customer_id,
           fund.fund_id,
           DATE_ADD('2025-01-06', INTERVAL days.day_no DAY) AS trade_date,
           CAST(SUBSTRING(customer.customer_id, 2) AS UNSIGNED) AS customer_no,
           CAST(SUBSTRING(fund.fund_id, 2) AS UNSIGNED) AS fund_no,
           days.day_no,
           CAST(SUBSTRING(customer.customer_id, 2) AS UNSIGNED) * 11
               + CAST(SUBSTRING(fund.fund_id, 2) AS UNSIGNED) * 7
               + days.day_no * 5 AS variant
    FROM demo_customer AS customer
    CROSS JOIN demo_fund AS fund
    CROSS JOIN (
        SELECT 0 AS day_no UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3
        UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7
        UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10 UNION ALL SELECT 11
        UNION ALL SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15
        UNION ALL SELECT 16 UNION ALL SELECT 17 UNION ALL SELECT 18 UNION ALL SELECT 19
        UNION ALL SELECT 20 UNION ALL SELECT 21 UNION ALL SELECT 22 UNION ALL SELECT 23
        UNION ALL SELECT 24 UNION ALL SELECT 25
    ) AS days
    WHERE customer.customer_id IN (
        'C001', 'C002', 'C003', 'C004', 'C005', 'C006', 'C007', 'C008', 'C009', 'C010'
    )
      AND fund.fund_id IN ('F001', 'F002', 'F003', 'F004', 'F005', 'F006')
      AND (customer.customer_id <> 'C001' OR fund.fund_id <> 'F001')
) AS seed
ON DUPLICATE KEY UPDATE trade_id = demo_fund_trade.trade_id;

COMMIT;

SELECT 'demo_customer' AS table_name, COUNT(*) AS row_count FROM demo_customer
UNION ALL
SELECT 'demo_fund', COUNT(*) FROM demo_fund
UNION ALL
SELECT 'demo_fund_trade', COUNT(*) FROM demo_fund_trade;
