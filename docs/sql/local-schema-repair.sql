/*
 * 本地旧库结构修复脚本。
 *
 * 适用场景：
 * - 应用能启动到 Tomcat，但定时任务反复报 Unknown column。
 * - 代码已经升级，MySQL 本地库 smart_ticket_lite 还停留在旧表结构。
 *
 * 执行方式：
 * MYSQL_PWD='你的MySQL密码' mysql --protocol=TCP -h 127.0.0.1 -P 3306 -u root -D smart_ticket_lite < docs/sql/local-schema-repair.sql
 */

DROP PROCEDURE IF EXISTS add_column_if_missing;

DELIMITER //

CREATE PROCEDURE add_column_if_missing(
    IN p_table_name VARCHAR(64),
    IN p_column_name VARCHAR(64),
    IN p_column_definition TEXT,
    IN p_after_column VARCHAR(64)
)
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = p_table_name
    ) AND NOT EXISTS (
        SELECT 1
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = p_table_name
          AND COLUMN_NAME = p_column_name
    ) THEN
        SET @ddl = CONCAT(
                'ALTER TABLE `', p_table_name, '` ADD COLUMN `', p_column_name, '` ', p_column_definition,
                CASE
                    WHEN p_after_column IS NULL OR p_after_column = '' THEN ''
                    ELSE CONCAT(' AFTER `', p_after_column, '`')
                END
            );
        PREPARE ddl_stmt FROM @ddl;
        EXECUTE ddl_stmt;
        DEALLOCATE PREPARE ddl_stmt;
    END IF;
END//

CREATE PROCEDURE add_index_if_missing(
    IN p_table_name VARCHAR(64),
    IN p_index_name VARCHAR(64),
    IN p_index_definition TEXT
)
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = p_table_name
    ) AND NOT EXISTS (
        SELECT 1
        FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = p_table_name
          AND INDEX_NAME = p_index_name
    ) THEN
        SET @ddl = CONCAT('ALTER TABLE `', p_table_name, '` ADD ', p_index_definition);
        PREPARE ddl_stmt FROM @ddl;
        EXECUTE ddl_stmt;
        DEALLOCATE PREPARE ddl_stmt;
    END IF;
END//

DELIMITER ;

CREATE TABLE IF NOT EXISTS audience_person (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    user_id BIGINT NOT NULL,
    name VARCHAR(64) NOT NULL,
    id_type VARCHAR(32) NOT NULL,
    id_no_ciphertext VARCHAR(512) NOT NULL,
    id_no_hash VARCHAR(128) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
    version INT NOT NULL DEFAULT 0,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    UNIQUE KEY uk_audience_user_id_hash (user_id, id_no_hash),
    KEY idx_audience_user_status (user_id, status)
);

CREATE TABLE IF NOT EXISTS ticket_purchase_plan (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    plan_no VARCHAR(64) NOT NULL,
    user_id BIGINT NOT NULL,
    show_id BIGINT NOT NULL,
    session_id BIGINT NULL,
    ticket_category_id BIGINT NULL,
    quantity INT NULL,
    status VARCHAR(32) NOT NULL,
    version INT NOT NULL DEFAULT 0,
    active_plan_key VARCHAR(128) NULL,
    completed_version INT NULL,
    default_audience_count INT NOT NULL DEFAULT 0,
    selected_audience_count INT NOT NULL DEFAULT 0,
    idempotency_token VARCHAR(128) NULL,
    order_request_id VARCHAR(128) NULL,
    order_id BIGINT NULL,
    expire_time DATETIME NULL,
    confirmed_at DATETIME NULL,
    completed_at DATETIME NULL,
    submitted_at DATETIME NULL,
    fail_reason VARCHAR(512) NULL,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    UNIQUE KEY uk_purchase_plan_no (plan_no),
    UNIQUE KEY uk_purchase_plan_active_key (active_plan_key),
    KEY idx_purchase_plan_user_status (user_id, status),
    KEY idx_purchase_plan_order_request (order_request_id),
    KEY idx_purchase_plan_expire (status, expire_time)
);

CREATE TABLE IF NOT EXISTS ticket_purchase_plan_audience (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    plan_id BIGINT NOT NULL,
    audience_id BIGINT NOT NULL,
    selection_type VARCHAR(32) NOT NULL,
    line_no INT NOT NULL,
    name_snapshot VARCHAR(64) NOT NULL,
    id_no_hash_snapshot VARCHAR(128) NOT NULL,
    created_at DATETIME NOT NULL,
    UNIQUE KEY uk_plan_audience_selection (plan_id, selection_type, audience_id),
    KEY idx_plan_audience_plan (plan_id, selection_type)
);

CREATE TABLE IF NOT EXISTS ticket_order_audience (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    order_id BIGINT NOT NULL,
    line_no INT NOT NULL,
    audience_id BIGINT NOT NULL,
    name_snapshot VARCHAR(64) NOT NULL,
    id_no_hash_snapshot VARCHAR(128) NOT NULL,
    created_at DATETIME NOT NULL,
    UNIQUE KEY uk_order_audience_line (order_id, line_no),
    UNIQUE KEY uk_order_audience_identity (order_id, audience_id),
    KEY idx_order_audience_order (order_id)
);

/* 异步创单请求表是预约计划提交和旧异步下单共用的核心表。旧库若缺表，后续 add_column_if_missing 无法修复，因此先补建。 */
CREATE TABLE IF NOT EXISTS ticket_order_request (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    request_id VARCHAR(128) NOT NULL,
    user_id BIGINT NOT NULL,
    show_id BIGINT NOT NULL,
    session_id BIGINT NOT NULL,
    ticket_category_id BIGINT NOT NULL,
    purchase_plan_id BIGINT NULL,
    quantity INT NOT NULL,
    status VARCHAR(32) NOT NULL,
    order_id BIGINT NULL,
    stock_bucket_version INT NULL DEFAULT 1,
    stock_bucket_no INT NULL,
    processing_at DATETIME NULL,
    redis_deducted TINYINT(1) NOT NULL DEFAULT 0,
    deducted_quantity INT NOT NULL DEFAULT 0,
    deducted_at DATETIME NULL,
    compensated TINYINT(1) NOT NULL DEFAULT 0,
    compensation_status VARCHAR(32) NOT NULL DEFAULT 'NONE',
    compensated_at DATETIME NULL,
    fail_reason VARCHAR(512) NULL,
    message_id VARCHAR(128) NULL,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    UNIQUE KEY uk_ticket_order_request_request_id (request_id),
    KEY idx_ticket_order_request_user_request (user_id, request_id),
    KEY idx_ticket_order_request_order_id (order_id),
    KEY idx_ticket_order_request_purchase_plan (purchase_plan_id),
    KEY idx_ticket_order_request_inflight_calc (ticket_category_id, status, redis_deducted, compensated, compensation_status, deducted_quantity),
    KEY idx_ticket_order_request_compensation_scan (status, redis_deducted, compensated, compensation_status, updated_at),
    KEY idx_ticket_order_request_bucket_version (ticket_category_id, stock_bucket_version, stock_bucket_no)
);

CREATE TABLE IF NOT EXISTS admin_operation_log (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    operator_user_id BIGINT NOT NULL,
    operator_username VARCHAR(64) NULL,
    operator_role VARCHAR(32) NOT NULL,
    operation_type VARCHAR(64) NOT NULL,
    resource_type VARCHAR(64) NULL,
    resource_id VARCHAR(64) NULL,
    request_uri VARCHAR(255) NULL,
    request_method VARCHAR(16) NULL,
    request_params TEXT NULL,
    operation_result VARCHAR(32) NOT NULL,
    error_message VARCHAR(512) NULL,
    client_ip VARCHAR(64) NULL,
    trace_id VARCHAR(64) NULL,
    created_at DATETIME NOT NULL,
    KEY idx_admin_operation_operator_time (operator_user_id, created_at),
    KEY idx_admin_operation_type_time (operation_type, created_at),
    KEY idx_admin_operation_resource (resource_type, resource_id),
    KEY idx_admin_operation_trace_id (trace_id)
);

CREATE TABLE IF NOT EXISTS dead_letter_message (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    message_id VARCHAR(128) NOT NULL,
    business_type VARCHAR(64) NULL,
    business_key VARCHAR(128) NULL,
    queue_name VARCHAR(128) NULL,
    exchange_name VARCHAR(128) NULL,
    routing_key VARCHAR(128) NULL,
    payload TEXT NULL,
    exception_type VARCHAR(128) NULL,
    exception_message TEXT NULL,
    status VARCHAR(32) NOT NULL,
    retry_count INT NOT NULL DEFAULT 0,
    max_retry_count INT NOT NULL DEFAULT 3,
    last_retry_at DATETIME NULL,
    resolved_at DATETIME NULL,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    KEY idx_dead_letter_status_created (status, created_at),
    KEY idx_dead_letter_message_id (message_id)
);

CREATE TABLE IF NOT EXISTS payment_callback_log (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    payment_no VARCHAR(64) NOT NULL,
    order_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    channel VARCHAR(32) NOT NULL,
    raw_body TEXT NULL,
    headers TEXT NULL,
    signature VARCHAR(255) NULL,
    verify_result VARCHAR(32) NULL,
    process_result VARCHAR(32) NULL,
    error_message VARCHAR(512) NULL,
    callback_time DATETIME NOT NULL,
    created_at DATETIME NOT NULL,
    KEY idx_payment_callback_payment_no (payment_no),
    KEY idx_payment_callback_order_id (order_id)
);

CREATE TABLE IF NOT EXISTS payment_flow_log (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    payment_no VARCHAR(64) NOT NULL,
    order_id BIGINT NOT NULL,
    from_status VARCHAR(32) NULL,
    to_status VARCHAR(32) NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    amount DECIMAL(10,2) NULL,
    result VARCHAR(32) NOT NULL,
    reason VARCHAR(512) NULL,
    created_at DATETIME NOT NULL,
    KEY idx_payment_flow_payment_no (payment_no),
    KEY idx_payment_flow_order_id (order_id)
);

CREATE TABLE IF NOT EXISTS ticket_stock_bucket (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    ticket_category_id BIGINT NOT NULL,
    bucket_version INT NOT NULL DEFAULT 1,
    bucket_no INT NOT NULL,
    total_stock INT NOT NULL DEFAULT 0,
    available_stock INT NOT NULL DEFAULT 0,
    locked_stock INT NOT NULL DEFAULT 0,
    sold_stock INT NOT NULL DEFAULT 0,
    version INT NOT NULL DEFAULT 0,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    UNIQUE KEY uk_ticket_bucket (ticket_category_id, bucket_version, bucket_no),
    KEY idx_ticket_category_version (ticket_category_id, bucket_version)
);

CREATE TABLE IF NOT EXISTS stock_adjustment_record (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    ticket_category_id BIGINT NOT NULL,
    operator_user_id BIGINT NOT NULL,
    adjust_quantity INT NOT NULL,
    before_available_stock INT NULL,
    after_available_stock INT NULL,
    before_redis_stock INT NULL,
    after_redis_stock INT NULL,
    reason VARCHAR(255) NULL,
    status VARCHAR(32) NOT NULL,
    confirm_token VARCHAR(64) NOT NULL,
    confirmed_at DATETIME NULL,
    rollback_available TINYINT(1) NOT NULL DEFAULT 0,
    rollback_record_id BIGINT NULL,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    KEY idx_stock_adjustment_ticket_time (ticket_category_id, created_at),
    KEY idx_stock_adjustment_status_time (status, created_at)
);

CREATE TABLE IF NOT EXISTS stock_compensation_record (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    ticket_category_id BIGINT NOT NULL,
    request_id VARCHAR(128) NULL,
    consistency_record_id BIGINT NULL,
    compensation_type VARCHAR(64) NOT NULL,
    before_redis_stock INT NULL,
    after_redis_stock INT NULL,
    mysql_available_stock INT NULL,
    in_flight_deducted_quantity INT NULL,
    expected_redis_available_stock INT NULL,
    delta INT NULL,
    status VARCHAR(32) NOT NULL,
    result_message VARCHAR(512) NULL,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    KEY idx_stock_compensation_status_time (status, created_at),
    KEY idx_stock_compensation_ticket_time (ticket_category_id, created_at),
    KEY idx_stock_compensation_request_id (request_id)
);

CREATE TABLE IF NOT EXISTS stock_consistency_record (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    ticket_category_id BIGINT NOT NULL,
    redis_available_stock INT NULL,
    mysql_available_stock INT NULL,
    mysql_locked_stock INT NULL,
    mysql_sold_stock INT NULL,
    in_flight_deducted_quantity INT NULL,
    expected_redis_available_stock INT NULL,
    diff INT NULL,
    status VARCHAR(32) NOT NULL,
    check_type VARCHAR(32) NOT NULL,
    repair_strategy VARCHAR(64) NULL,
    repair_result VARCHAR(512) NULL,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    repaired_at DATETIME NULL,
    KEY idx_stock_consistency_status_time (status, created_at),
    KEY idx_stock_consistency_ticket_time (ticket_category_id, created_at)
);

CALL add_column_if_missing('ticket_category', 'status', 'varchar(32) NOT NULL DEFAULT ''PUBLISHED''', 'price');

CALL add_column_if_missing('user_account', 'role_code', 'varchar(32) NOT NULL DEFAULT ''USER''', 'status');

CALL add_column_if_missing('local_message', 'confirmed_at', 'datetime NULL', 'sent_at');
-- returned_at 是旧 RabbitMQ ReturnCallback 语义遗留；历史开发库若已存在可保留，但当前模型不再创建或使用。
CALL add_column_if_missing('local_message', 'dead_at', 'datetime NULL', 'confirmed_at');

CALL add_column_if_missing('stock_adjustment_record', 'confirmed_at', 'datetime NULL', 'confirm_token');
CALL add_column_if_missing('stock_adjustment_record', 'rollback_available', 'tinyint(1) NOT NULL DEFAULT 0', 'confirmed_at');
CALL add_column_if_missing('stock_adjustment_record', 'rollback_record_id', 'bigint NULL', 'rollback_available');

CALL add_column_if_missing('ticket_order', 'show_title', 'varchar(128) NULL', 'quantity');
CALL add_column_if_missing('ticket_order', 'session_start_time', 'datetime NULL', 'show_title');
CALL add_column_if_missing('ticket_order', 'ticket_category_name', 'varchar(64) NULL', 'session_start_time');
CALL add_column_if_missing('ticket_order', 'ticket_price', 'decimal(10,2) NULL', 'ticket_category_name');
CALL add_column_if_missing('ticket_order', 'total_amount', 'decimal(10,2) NULL', 'ticket_price');
CALL add_column_if_missing('ticket_order', 'cancel_reason', 'varchar(255) NULL', 'close_time');
CALL add_column_if_missing('ticket_order', 'version', 'int NOT NULL DEFAULT 0', 'cancel_reason');

CALL add_column_if_missing('ticket_purchase_plan', 'active_plan_key', 'varchar(128) NULL', 'version');
CALL add_column_if_missing('ticket_purchase_plan', 'completed_version', 'int NULL', 'active_plan_key');
CALL add_column_if_missing('ticket_purchase_plan', 'completed_at', 'datetime NULL', 'confirmed_at');

CALL add_column_if_missing('performance_session', 'sale_start_time', 'datetime NULL', 'end_time');
CALL add_column_if_missing('performance_session', 'sale_end_time', 'datetime NULL', 'sale_start_time');

CALL add_column_if_missing('ticket_order_request', 'stock_bucket_version', 'int NOT NULL DEFAULT 1', 'order_id');
CALL add_column_if_missing('ticket_order_request', 'purchase_plan_id', 'bigint NULL', 'ticket_category_id');
CALL add_column_if_missing('ticket_order_request', 'stock_bucket_no', 'int NULL', 'stock_bucket_version');
CALL add_column_if_missing('ticket_order_request', 'processing_at', 'datetime NULL', 'stock_bucket_no');
CALL add_column_if_missing('ticket_order_request', 'redis_deducted', 'tinyint(1) NOT NULL DEFAULT 0', 'processing_at');
CALL add_column_if_missing('ticket_order_request', 'deducted_quantity', 'int NOT NULL DEFAULT 0', 'redis_deducted');
CALL add_column_if_missing('ticket_order_request', 'deducted_at', 'datetime NULL', 'deducted_quantity');
CALL add_column_if_missing('ticket_order_request', 'compensated', 'tinyint(1) NOT NULL DEFAULT 0', 'deducted_at');
CALL add_column_if_missing('ticket_order_request', 'compensation_status', 'varchar(32) NOT NULL DEFAULT ''NONE''', 'compensated');
CALL add_column_if_missing('ticket_order_request', 'compensated_at', 'datetime NULL', 'compensation_status');
CALL add_column_if_missing('ticket_order_request', 'message_id', 'varchar(128) NULL', 'fail_reason');

CALL add_index_if_missing('ticket_stock', 'uk_ticket_stock_category', 'UNIQUE KEY `uk_ticket_stock_category` (`ticket_category_id`)');
CALL add_index_if_missing('ticket_purchase_plan', 'uk_purchase_plan_active_key', 'UNIQUE KEY `uk_purchase_plan_active_key` (`active_plan_key`)');
CALL add_index_if_missing('performance_session', 'idx_session_sale_window', 'KEY `idx_session_sale_window` (`status`, `sale_start_time`, `sale_end_time`)');

CALL add_index_if_missing('ticket_order_request', 'uk_ticket_order_request_request_id', 'UNIQUE KEY `uk_ticket_order_request_request_id` (`request_id`)');
CALL add_index_if_missing('ticket_order_request', 'idx_ticket_order_request_user_request', 'KEY `idx_ticket_order_request_user_request` (`user_id`, `request_id`)');
CALL add_index_if_missing('ticket_order_request', 'idx_ticket_order_request_order_id', 'KEY `idx_ticket_order_request_order_id` (`order_id`)');
CALL add_index_if_missing('ticket_order_request', 'idx_ticket_order_request_purchase_plan', 'KEY `idx_ticket_order_request_purchase_plan` (`purchase_plan_id`)');
CALL add_index_if_missing('ticket_order_request', 'idx_ticket_order_request_inflight_calc', 'KEY `idx_ticket_order_request_inflight_calc` (`ticket_category_id`, `status`, `redis_deducted`, `compensated`, `compensation_status`, `deducted_quantity`)');
CALL add_index_if_missing('ticket_order_request', 'idx_ticket_order_request_compensation_scan', 'KEY `idx_ticket_order_request_compensation_scan` (`status`, `redis_deducted`, `compensated`, `compensation_status`, `updated_at`)');
CALL add_index_if_missing('ticket_order_request', 'idx_ticket_order_request_bucket_version', 'KEY `idx_ticket_order_request_bucket_version` (`ticket_category_id`, `stock_bucket_version`, `stock_bucket_no`)');

CALL add_index_if_missing('ticket_order', 'uk_ticket_order_order_no', 'UNIQUE KEY `uk_ticket_order_order_no` (`order_no`)');
CALL add_index_if_missing('ticket_order', 'idx_ticket_order_user_created', 'KEY `idx_ticket_order_user_created` (`user_id`, `created_at`)');
CALL add_index_if_missing('ticket_order', 'idx_ticket_order_expire_scan', 'KEY `idx_ticket_order_expire_scan` (`status`, `expire_time`)');
CALL add_index_if_missing('ticket_order', 'idx_ticket_order_ticket_status', 'KEY `idx_ticket_order_ticket_status` (`ticket_category_id`, `status`)');
CALL add_index_if_missing('ticket_order', 'idx_ticket_order_show_status', 'KEY `idx_ticket_order_show_status` (`show_id`, `status`)');
CALL add_index_if_missing('ticket_order', 'idx_ticket_order_session_status', 'KEY `idx_ticket_order_session_status` (`session_id`, `status`)');

CALL add_index_if_missing('local_message', 'uk_local_message_message_id', 'UNIQUE KEY `uk_local_message_message_id` (`message_id`)');
CALL add_index_if_missing('local_message', 'idx_local_message_status_updated_at', 'KEY `idx_local_message_status_updated_at` (`status`, `updated_at`)');
CALL add_index_if_missing('local_message', 'idx_local_message_business', 'KEY `idx_local_message_business` (`business_type`, `business_key`)');

UPDATE ticket_category
SET status = 'PUBLISHED'
WHERE status IS NULL OR status = '';

UPDATE user_account
SET role_code = 'USER'
WHERE role_code IS NULL OR role_code = '';

UPDATE ticket_order
SET version = 0
WHERE version IS NULL;

DROP PROCEDURE IF EXISTS add_column_if_missing;
DROP PROCEDURE IF EXISTS add_index_if_missing;
