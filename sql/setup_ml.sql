-- Keep the FAR-Trans project alongside the existing ETL demo.
-- Run with: mysql --database=spark_project_demo < sql/setup_ml.sql

CREATE TABLE IF NOT EXISTS far_dataset (
    data_version VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    customer_rows BIGINT NOT NULL,
    asset_rows BIGINT NOT NULL,
    trade_rows BIGINT NOT NULL,
    coverage_end DATE NOT NULL,
    PRIMARY KEY (data_version)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS far_customer (
    customer_id VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    profile_date DATE NOT NULL,
    customer_type VARCHAR(64) NOT NULL,
    risk_level VARCHAR(64) NOT NULL,
    investment_capacity VARCHAR(64) NOT NULL,
    PRIMARY KEY (customer_id, profile_date)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS far_asset (
    isin VARCHAR(12) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    asset_date DATE NOT NULL,
    fund_category VARCHAR(64) NOT NULL,
    first_price_date DATE NOT NULL,
    last_price_date DATE NOT NULL,
    PRIMARY KEY (isin, asset_date)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS far_trade (
    customer_id VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    isin VARCHAR(12) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    trade_date DATE NOT NULL,
    trade_type VARCHAR(4) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    transaction_id VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    amount DECIMAL(32,18) NOT NULL,
    PRIMARY KEY (customer_id, isin, trade_date, trade_type, transaction_id),
    KEY ix_far_trade_window (trade_type, trade_date, isin)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS fund_ml_prediction (
    run_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    customer_id VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    isin VARCHAR(12) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    as_of_date DATE NOT NULL,
    label DOUBLE NOT NULL,
    probability DOUBLE NOT NULL,
    prediction DOUBLE NOT NULL,
    PRIMARY KEY (run_id, customer_id, isin, as_of_date)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS fund_ml_metric (
    run_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    metric_name VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    metric_value DOUBLE NOT NULL,
    PRIMARY KEY (run_id, metric_name)
) ENGINE=InnoDB;

-- A run is registered only after the model and its outputs are ready.
CREATE TABLE IF NOT EXISTS fund_ml_run (
    run_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    code_version VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    spark_app_id VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    data_version VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    model_uri VARCHAR(512) NOT NULL,
    train_rows BIGINT NOT NULL,
    validation_rows BIGINT NOT NULL,
    test_rows BIGINT NOT NULL,
    threshold DOUBLE NOT NULL,
    sample_signature VARCHAR(512) NULL,
    model_checksum VARCHAR(64) NULL,
    model_bytes BIGINT NULL,
    log_path VARCHAR(255) NULL,
    completed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (run_id)
) ENGINE=InnoDB;

-- Existing ML databases predate the audit columns; add them once each.
SET @ddl = (
    SELECT IF(COUNT(*) = 0,
              'ALTER TABLE fund_ml_run ADD COLUMN sample_signature VARCHAR(512) NULL',
              'SET @demo_noop = 1')
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'fund_ml_run' AND column_name = 'sample_signature'
);
PREPARE demo_ddl FROM @ddl;
EXECUTE demo_ddl;
DEALLOCATE PREPARE demo_ddl;

SET @ddl = (
    SELECT IF(COUNT(*) = 0,
              'ALTER TABLE fund_ml_run ADD COLUMN model_checksum VARCHAR(64) NULL',
              'SET @demo_noop = 1')
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'fund_ml_run' AND column_name = 'model_checksum'
);
PREPARE demo_ddl FROM @ddl;
EXECUTE demo_ddl;
DEALLOCATE PREPARE demo_ddl;

SET @ddl = (
    SELECT IF(COUNT(*) = 0,
              'ALTER TABLE fund_ml_run ADD COLUMN model_bytes BIGINT NULL',
              'SET @demo_noop = 1')
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'fund_ml_run' AND column_name = 'model_bytes'
);
PREPARE demo_ddl FROM @ddl;
EXECUTE demo_ddl;
DEALLOCATE PREPARE demo_ddl;

SET @ddl = (
    SELECT IF(COUNT(*) = 0,
              'ALTER TABLE fund_ml_run ADD COLUMN log_path VARCHAR(255) NULL',
              'SET @demo_noop = 1')
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'fund_ml_run' AND column_name = 'log_path'
);
PREPARE demo_ddl FROM @ddl;
EXECUTE demo_ddl;
DEALLOCATE PREPARE demo_ddl;
