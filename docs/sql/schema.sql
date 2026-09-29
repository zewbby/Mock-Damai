DROP TABLE IF EXISTS stock_compensation_record;
DROP TABLE IF EXISTS stock_consistency_record;
DROP TABLE IF EXISTS stock_adjustment_record;
DROP TABLE IF EXISTS dead_letter_message;
DROP TABLE IF EXISTS payment_flow_log;
DROP TABLE IF EXISTS payment_callback_log;
DROP TABLE IF EXISTS payment_order;
DROP TABLE IF EXISTS local_message;
DROP TABLE IF EXISTS ticket_order_audience;
DROP TABLE IF EXISTS ticket_purchase_plan_audience;
DROP TABLE IF EXISTS ticket_purchase_plan;
DROP TABLE IF EXISTS audience_person;
DROP TABLE IF EXISTS ticket_order_request;
DROP TABLE IF EXISTS ticket_order;
DROP TABLE IF EXISTS ticket_stock_bucket;
DROP TABLE IF EXISTS ticket_stock;
DROP TABLE IF EXISTS ticket_category;
DROP TABLE IF EXISTS performance_session;
DROP TABLE IF EXISTS show_info;
DROP TABLE IF EXISTS venue;
DROP TABLE IF EXISTS admin_operation_log;
DROP TABLE IF EXISTS user_account;

CREATE TABLE user_account (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    username VARCHAR(64) NOT NULL,
    phone VARCHAR(32) NOT NULL,
    password VARCHAR(255) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'NORMAL',
    role_code VARCHAR(32) NOT NULL DEFAULT 'USER',
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    UNIQUE KEY uk_user_phone (phone),
    UNIQUE KEY uk_user_username (username)
);

CREATE TABLE audience_person (
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

CREATE TABLE venue (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(128) NOT NULL,
    city VARCHAR(64) NOT NULL,
    address VARCHAR(255) NOT NULL,
    capacity INT NOT NULL,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL
);

CREATE TABLE show_info (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    title VARCHAR(128) NOT NULL,
    artist VARCHAR(128) NOT NULL,
    venue_id BIGINT NOT NULL,
    description VARCHAR(512) NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'DRAFT' COMMENT 'DRAFT/PUBLISHED/OFFLINE',
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    KEY idx_show_status (status)
);

CREATE TABLE performance_session (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    show_id BIGINT NOT NULL,
    start_time DATETIME NOT NULL,
    end_time DATETIME NOT NULL,
    sale_start_time DATETIME NOT NULL,
    sale_end_time DATETIME NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'DRAFT' COMMENT 'DRAFT/PUBLISHED/OFFLINE',
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    KEY idx_session_show_status (show_id, status),
    KEY idx_session_sale_window (status, sale_start_time, sale_end_time)
);

CREATE TABLE ticket_category (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    session_id BIGINT NOT NULL,
    category_name VARCHAR(64) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'DRAFT' COMMENT 'DRAFT/PUBLISHED/OFFLINE/SOLD_OUT',
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    KEY idx_ticket_category_session_status (session_id, status)
);

CREATE TABLE ticket_stock (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    ticket_category_id BIGINT NOT NULL,
    total_stock INT NOT NULL DEFAULT 0,
    available_stock INT NOT NULL DEFAULT 0,
    locked_stock INT NOT NULL DEFAULT 0,
    sold_stock INT NOT NULL DEFAULT 0,
    version INT NOT NULL DEFAULT 0,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    UNIQUE KEY uk_ticket_stock_category (ticket_category_id)
);

CREATE TABLE ticket_stock_bucket (
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

CREATE TABLE ticket_purchase_plan (
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

CREATE TABLE ticket_purchase_plan_audience (
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

CREATE TABLE ticket_order (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    order_no VARCHAR(64) NOT NULL,
    user_id BIGINT NOT NULL,
    show_id BIGINT NOT NULL,
    session_id BIGINT NOT NULL,
    ticket_category_id BIGINT NOT NULL,
    quantity INT NOT NULL,
    show_title VARCHAR(128) NOT NULL,
    session_start_time DATETIME NOT NULL,
    ticket_category_name VARCHAR(64) NOT NULL,
    ticket_price DECIMAL(10,2) NOT NULL,
    total_amount DECIMAL(10,2) NOT NULL,
    status VARCHAR(32) NOT NULL,
    expire_time DATETIME NULL,
    pay_time DATETIME NULL,
    cancel_time DATETIME NULL,
    close_time DATETIME NULL,
    cancel_reason VARCHAR(255) NULL,
    version INT NOT NULL DEFAULT 0,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    UNIQUE KEY uk_ticket_order_order_no (order_no),
    KEY idx_ticket_order_user_created (user_id, created_at),
    KEY idx_ticket_order_expire_scan (status, expire_time),
    KEY idx_ticket_order_ticket_status (ticket_category_id, status),
    KEY idx_ticket_order_show_status (show_id, status),
    KEY idx_ticket_order_session_status (session_id, status)
);

CREATE TABLE ticket_order_audience (
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

CREATE TABLE ticket_order_request (
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

CREATE TABLE local_message (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    message_id VARCHAR(128) NOT NULL,
    business_type VARCHAR(64) NOT NULL,
    business_key VARCHAR(128) NOT NULL,
    exchange_name VARCHAR(128) NULL,
    routing_key VARCHAR(128) NULL,
    payload TEXT NOT NULL,
    status VARCHAR(32) NOT NULL,
    retry_count INT NOT NULL DEFAULT 0,
    max_retry_count INT NOT NULL DEFAULT 3,
    next_retry_time DATETIME NULL,
    last_error VARCHAR(512) NULL,
    sent_at DATETIME NULL,
    confirmed_at DATETIME NULL,
    dead_at DATETIME NULL,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    UNIQUE KEY uk_local_message_message_id (message_id),
    KEY idx_status_updated_at (status, updated_at),
    KEY idx_local_message_publish_scan (status, next_retry_time, created_at),
    KEY idx_local_message_business (business_type, business_key)
);

CREATE TABLE payment_order (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    payment_no VARCHAR(64) NOT NULL,
    order_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    channel VARCHAR(32) NOT NULL,
    status VARCHAR(32) NOT NULL,
    paid_at DATETIME NULL,
    callback_at DATETIME NULL,
    closed_at DATETIME NULL,
    fail_reason VARCHAR(512) NULL,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    UNIQUE KEY uk_payment_no (payment_no),
    UNIQUE KEY uk_order_id (order_id),
    KEY idx_payment_user_status (user_id, status)
);

CREATE TABLE payment_callback_log (
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

CREATE TABLE payment_flow_log (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    payment_no VARCHAR(64) NOT NULL,
    order_id BIGINT NOT NULL,
    from_status VARCHAR(32) NULL,
    to_status VARCHAR(32) NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    result VARCHAR(32) NOT NULL,
    reason VARCHAR(512) NULL,
    created_at DATETIME NOT NULL,
    KEY idx_payment_flow_payment_no (payment_no),
    KEY idx_payment_flow_order_id (order_id)
);

CREATE TABLE admin_operation_log (
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

CREATE TABLE dead_letter_message (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    message_id VARCHAR(128) NOT NULL,
    business_type VARCHAR(64) NULL,
    business_key VARCHAR(128) NULL,
    queue_name VARCHAR(128) NULL,
    exchange_name VARCHAR(128) NULL,
    routing_key VARCHAR(128) NULL,
    payload TEXT NULL,
    exception_type VARCHAR(64) NOT NULL,
    exception_message TEXT NULL,
    status VARCHAR(32) NOT NULL,
    retry_count INT NOT NULL DEFAULT 0,
    max_retry_count INT NOT NULL DEFAULT 3,
    last_retry_at DATETIME NULL,
    resolved_at DATETIME NULL,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    KEY idx_dead_letter_status_created (status, created_at),
    KEY idx_dead_letter_business_key (business_key),
    KEY idx_dead_letter_message_id (message_id)
);

CREATE TABLE stock_adjustment_record (
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

CREATE TABLE stock_consistency_record (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    ticket_category_id BIGINT NOT NULL,
    redis_available_stock INT NULL,
    mysql_available_stock INT NULL,
    mysql_locked_stock INT NULL,
    mysql_sold_stock INT NULL,
    in_flight_deducted_quantity INT NULL,
    expected_redis_available_stock INT NOT NULL,
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

CREATE TABLE stock_compensation_record (
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
