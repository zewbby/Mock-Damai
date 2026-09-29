package com.zewbby.smartticket.mapper;

import com.zewbby.smartticket.constant.OrderConstant;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class MapperSqlContractTest {

    @Test
    void ticketStockDecreaseSqlDeductsAvailableStockAndPreventsNegativeStock() throws Exception {
        String xml = Files.readString(Path.of("src/main/resources/mapper/TicketStockMapper.xml"));

        assertThat(xml).contains("available_stock = available_stock - #{quantity}");
        assertThat(xml).contains("locked_stock = locked_stock + #{quantity}");
        assertThat(xml).contains("AND available_stock >= #{quantity}");
    }

    @Test
    void ticketStockRollbackSqlReleasesOnlyLockedStockAndPreventsOverRelease() throws Exception {
        String xml = Files.readString(Path.of("src/main/resources/mapper/TicketStockMapper.xml"));

        assertThat(xml).contains("available_stock = available_stock + #{quantity}");
        assertThat(xml).contains("locked_stock = locked_stock - #{quantity}");
        assertThat(xml).contains("AND locked_stock >= #{quantity}");
    }

    @Test
    void relationValidationSqlChecksShowSessionTicketCategoryChain() throws Exception {
        String xml = Files.readString(Path.of("src/main/resources/mapper/TicketCategoryMapper.xml"));

        assertThat(xml).contains("JOIN performance_session ps ON ps.show_id = s.id");
        assertThat(xml).contains("JOIN ticket_category tc ON tc.session_id = ps.id");
        assertThat(xml).contains("s.id = #{showId}");
        assertThat(xml).contains("ps.id = #{sessionId}");
        assertThat(xml).contains("tc.id = #{ticketCategoryId}");
        assertThat(xml).contains("s.status = 'PUBLISHED'");
        assertThat(xml).contains("ps.status = 'PUBLISHED'");
        assertThat(xml).contains("tc.status = 'PUBLISHED'");
    }

    @Test
    void publicTicketCategorySqlRequiresPublishedShowAndSession() throws Exception {
        String xml = Files.readString(Path.of("src/main/resources/mapper/ShowMapper.xml"));

        assertThat(xml).contains("JOIN performance_session ps ON ps.id = tc.session_id");
        assertThat(xml).contains("JOIN show_info si ON si.id = ps.show_id");
        assertThat(xml).contains("AND ps.status = 'PUBLISHED'");
        assertThat(xml).contains("AND si.status = 'PUBLISHED'");
    }

    @Test
    void adminBusinessSqlContainsResourceStatusesAndUserSidePublishedFilters() throws Exception {
        String schema = Files.readString(Path.of("docs/sql/schema.sql"));
        String showXml = Files.readString(Path.of("src/main/resources/mapper/ShowMapper.xml"));
        String ticketCategoryXml = Files.readString(Path.of("src/main/resources/mapper/TicketCategoryMapper.xml"));
        String ticketStockXml = Files.readString(Path.of("src/main/resources/mapper/TicketStockMapper.xml"));

        assertThat(schema).contains("status VARCHAR(32) NOT NULL DEFAULT 'DRAFT' COMMENT 'DRAFT/PUBLISHED/OFFLINE'");
        assertThat(schema).contains("status VARCHAR(32) NOT NULL DEFAULT 'DRAFT' COMMENT 'DRAFT/PUBLISHED/OFFLINE/SOLD_OUT'");
        assertThat(showXml).contains("WHERE si.status = 'PUBLISHED'");
        assertThat(showXml).contains("AND ps.status = 'PUBLISHED'");
        assertThat(showXml).contains("AND tc.status = 'PUBLISHED'");
        assertThat(ticketCategoryXml).contains("<result property=\"status\" column=\"status\"/>");
        assertThat(ticketCategoryXml).contains("<select id=\"adminSelectBySessionId\"");
        assertThat(ticketStockXml).contains("<insert id=\"insert\"");
        assertThat(ticketStockXml).contains("<update id=\"initExistingStock\">");
        assertThat(ticketStockXml).contains("<update id=\"adjustAvailableStock\">");
    }

    @Test
    void sessionSqlContainsSaleWindowFields() throws Exception {
        String schema = Files.readString(Path.of("docs/sql/schema.sql"));
        String repair = Files.readString(Path.of("docs/sql/local-schema-repair.sql"));
        String showXml = Files.readString(Path.of("src/main/resources/mapper/ShowMapper.xml"));

        assertThat(schema).contains("sale_start_time DATETIME");
        assertThat(schema).contains("sale_end_time DATETIME");
        assertThat(repair).contains("sale_start_time");
        assertThat(repair).contains("sale_end_time");
        assertThat(showXml).contains("sale_start_time");
        assertThat(showXml).contains("sale_end_time");
    }

    @Test
    void purchasePlanSqlUsesMysqlCompatibleUpdatesAndExactRequestBinding() throws Exception {
        String xml = Files.readString(Path.of("src/main/resources/mapper/TicketPurchasePlanMapper.xml"));

        assertThat(xml).doesNotContainPattern("(?m)^\\s*SET\\s+p\\.");
        assertThat(xml).doesNotContainPattern("(?m)^\\s*p\\.[a-z_]+\\s*=");
        assertThat(xml).doesNotContain("OR order_request_id IS NULL");
        assertThat(xml).contains("ps.sale_start_time &lt;= #{now}");
        assertThat(xml).contains("ps.sale_end_time &lt;= #{now}");
        assertThat(xml).doesNotContain("ps.sale_start_time <= #{now}");
        assertThat(xml).doesNotContain("ps.sale_end_time <= #{now}");

        assertThat(mapperStatement(xml, "markOrderCreated"))
                .contains("AND order_request_id = #{orderRequestId}");
        assertThat(mapperStatement(xml, "markFailed"))
                .contains("AND order_request_id = #{orderRequestId}");
        assertThat(mapperStatement(xml, "markReconciliationRequired"))
                .contains("AND order_request_id = #{orderRequestId}")
                .contains("status IN ('SUBMITTING', 'RECONCILIATION_REQUIRED')");
    }

    @Test
    void purchasePlanExpirySqlKeepsEditableAndReadyStateBoundaries() throws Exception {
        String xml = Files.readString(Path.of("src/main/resources/mapper/TicketPurchasePlanMapper.xml"));

        assertThat(mapperStatement(xml, "expireEditable"))
                .contains("WHERE p.status = 'DRAFT'")
                .contains("ps.sale_start_time &lt;= #{now}")
                .contains("active_plan_key = NULL");
        assertThat(mapperStatement(xml, "expireReadyAfterSale"))
                .contains("WHERE p.status = 'READY'")
                .contains("ps.sale_end_time &lt;= #{now}")
                .contains("active_plan_key = NULL");
    }

    private String mapperStatement(String xml, String id) {
        String startTag = "<update id=\"" + id + "\">";
        int start = xml.indexOf(startTag);
        assertThat(start).as("mapper statement %s exists", id).isGreaterThanOrEqualTo(0);
        int end = xml.indexOf("</update>", start);
        assertThat(end).as("mapper statement %s is closed", id).isGreaterThan(start);
        return xml.substring(start, end);
    }

    @Test
    void orderTimeoutConstantsUseSingleSourceForMinutesAndMillis() {
        assertThat(OrderConstant.ORDER_TIMEOUT_TTL_MILLIS)
                .isEqualTo(OrderConstant.ORDER_TIMEOUT_MINUTES * 60 * 1000);
    }

    @Test
    void userSqlContainsPasswordAndStatusFields() throws Exception {
        String schema = Files.readString(Path.of("docs/sql/schema.sql"));
        String xml = Files.readString(Path.of("src/main/resources/mapper/UserMapper.xml"));

        assertThat(schema).contains("password VARCHAR(255) NOT NULL");
        assertThat(schema).contains("status VARCHAR(32) NOT NULL DEFAULT 'NORMAL'");
        assertThat(schema).contains("role_code VARCHAR(32) NOT NULL DEFAULT 'USER'");
        assertThat(xml).contains("<result property=\"password\" column=\"password\"/>");
        assertThat(xml).contains("<result property=\"status\" column=\"status\"/>");
        assertThat(xml).contains("<result property=\"roleCode\" column=\"role_code\"/>");
    }

    @Test
    void adminOperationLogSqlContainsAuditFieldsAndIndexes() throws Exception {
        String schema = Files.readString(Path.of("docs/sql/schema.sql"));
        String xml = Files.readString(Path.of("src/main/resources/mapper/AdminOperationLogMapper.xml"));

        assertThat(schema).contains("CREATE TABLE admin_operation_log");
        assertThat(schema).contains("operator_user_id BIGINT NOT NULL");
        assertThat(schema).contains("operation_type VARCHAR(64) NOT NULL");
        assertThat(schema).contains("operation_result VARCHAR(32) NOT NULL");
        assertThat(schema).contains("trace_id VARCHAR(64) NULL");
        assertThat(schema).contains("KEY idx_admin_operation_trace_id (trace_id)");
        assertThat(xml).contains("INSERT INTO admin_operation_log");
        assertThat(xml).contains("operator_role");
        assertThat(xml).contains("request_params");
    }

    @Test
    void userSideOrderSqlUsesUserIdConditionsForReadPayAndCancel() throws Exception {
        String orderXml = Files.readString(Path.of("src/main/resources/mapper/OrderMapper.xml"));
        String requestXml = Files.readString(Path.of("src/main/resources/mapper/OrderRequestMapper.xml"));

        assertThat(orderXml).contains("<select id=\"selectByIdAndUserId\"");
        assertThat(orderXml).contains("<update id=\"updateCancelStatusByUserId\">");
        assertThat(orderXml).contains("<update id=\"updatePayStatusByUserId\">");
        assertThat(orderXml).contains("AND user_id = #{userId}");
        assertThat(requestXml).contains("<select id=\"selectByRequestIdAndUserId\"");
        assertThat(requestXml).contains("AND user_id = #{userId}");
    }

    @Test
    void paymentSqlContainsPaymentOrderTableAndIdempotentStatusConditions() throws Exception {
        String schema = Files.readString(Path.of("docs/sql/schema.sql"));
        String paymentXml = Files.readString(Path.of("src/main/resources/mapper/PaymentMapper.xml"));

        assertThat(schema).contains("CREATE TABLE payment_order");
        assertThat(schema).contains("CREATE TABLE payment_callback_log");
        assertThat(schema).contains("CREATE TABLE payment_flow_log");
        assertThat(schema).contains("show_title VARCHAR(128) NOT NULL");
        assertThat(schema).contains("session_start_time DATETIME NOT NULL");
        assertThat(schema).contains("ticket_category_name VARCHAR(64) NOT NULL");
        assertThat(schema).contains("ticket_price DECIMAL(10,2) NOT NULL");
        assertThat(schema).contains("total_amount DECIMAL(10,2) NOT NULL");
        assertThat(schema).contains("amount DECIMAL(10,2) NOT NULL");
        assertThat(schema).contains("UNIQUE KEY uk_order_id (order_id)");
        assertThat(paymentXml).contains("<select id=\"selectByOrderId\"");
        assertThat(paymentXml).contains("WHERE payment_no = #{paymentNo}");
        assertThat(paymentXml).contains("AND status IN ('INIT', 'PAYING')");
        assertThat(paymentXml).contains("WHERE payment_no = #{paymentNo}\n          AND user_id = #{userId}");
    }

    @Test
    void orderSnapshotSqlContainsSnapshotFieldsAndJoinQuery() throws Exception {
        String orderXml = Files.readString(Path.of("src/main/resources/mapper/OrderMapper.xml"));
        String ticketCategoryXml = Files.readString(Path.of("src/main/resources/mapper/TicketCategoryMapper.xml"));

        assertThat(orderXml).contains("show_title");
        assertThat(orderXml).contains("session_start_time");
        assertThat(orderXml).contains("ticket_category_name");
        assertThat(orderXml).contains("ticket_price");
        assertThat(ticketCategoryXml).contains("<select id=\"selectOrderSnapshot\"");
        assertThat(ticketCategoryXml).contains("s.title AS show_title");
        assertThat(ticketCategoryXml).contains("ps.start_time AS session_start_time");
        assertThat(ticketCategoryXml).contains("tc.category_name AS ticket_category_name");
        assertThat(ticketCategoryXml).contains("tc.price AS ticket_price");
    }

    @Test
    void orderRequestSqlContainsRedisPreDeductFieldsAndStateTransitions() throws Exception {
        String schema = Files.readString(Path.of("docs/sql/schema.sql"));
        String requestXml = Files.readString(Path.of("src/main/resources/mapper/OrderRequestMapper.xml"));

        assertThat(schema).contains("redis_deducted TINYINT(1) NOT NULL DEFAULT 0");
        assertThat(schema).contains("deducted_quantity INT NOT NULL DEFAULT 0");
        assertThat(schema).contains("stock_bucket_version INT NULL DEFAULT 1");
        assertThat(schema).contains("compensated TINYINT(1) NOT NULL DEFAULT 0");
        assertThat(schema).contains("processing_at DATETIME NULL");
        assertThat(schema).contains("compensation_status VARCHAR(32) NOT NULL DEFAULT 'NONE'");
        assertThat(schema).contains("KEY idx_ticket_order_request_inflight_calc");
        assertThat(requestXml).contains("<update id=\"markPreDeducted\">");
        assertThat(requestXml).contains("stock_bucket_version");
        assertThat(requestXml).contains("status = 'PRE_DEDUCTED'");
        assertThat(requestXml).contains("<update id=\"tryMarkProcessing\">");
        assertThat(requestXml).contains("AND status IN ('PRE_DEDUCTED', 'QUEUED')");
        assertThat(requestXml).contains("processing_at = NOW()");
        assertThat(requestXml).contains("<update id=\"tryMarkCompensating\">");
        assertThat(requestXml).contains("<update id=\"markCompensated\">");
        assertThat(requestXml).contains("<select id=\"sumInFlightDeductedQuantity\"");
        assertThat(requestXml).contains("status IN ('PRE_DEDUCTED', 'QUEUED', 'PROCESSING')");
    }

    @Test
    void bucketVersionSchemaAndMapperContainVersionCoordinates() throws Exception {
        String schema = Files.readString(Path.of("docs/sql/schema.sql"));
        String bucketXml = Files.readString(Path.of("src/main/resources/mapper/TicketStockBucketMapper.xml"));
        String requestXml = Files.readString(Path.of("src/main/resources/mapper/OrderRequestMapper.xml"));

        assertThat(schema).contains("bucket_version INT NOT NULL DEFAULT 1");
        assertThat(schema).contains("UNIQUE KEY uk_ticket_bucket (ticket_category_id, bucket_version, bucket_no)");
        assertThat(schema).contains("KEY idx_ticket_category_version (ticket_category_id, bucket_version)");
        assertThat(schema).contains("KEY idx_ticket_order_request_bucket_version");
        assertThat(bucketXml).contains("<result property=\"bucketVersion\" column=\"bucket_version\"/>");
        assertThat(bucketXml).contains("<select id=\"selectByVersionBucket\"");
        assertThat(bucketXml).contains("<select id=\"selectByTicketCategoryIdAndVersion\"");
        assertThat(bucketXml).contains("<update id=\"decreaseStockByVersion\">");
        assertThat(bucketXml).contains("<update id=\"rollbackStockByVersion\">");
        assertThat(bucketXml).contains("<update id=\"confirmStockByVersion\">");
        assertThat(requestXml).contains("<result property=\"stockBucketVersion\" column=\"stock_bucket_version\"/>");
    }

    @Test
    void redisPreDeductLuaContainsRequestIdDedupAndReleaseCompensation() throws Exception {
        String preDeductLua = Files.readString(Path.of("src/main/resources/lua/stock_pre_deduct.lua"));
        String releaseLua = Files.readString(Path.of("src/main/resources/lua/stock_release_pre_deduct.lua"));

        assertThat(preDeductLua).contains("ticket:stock:{ticketCategoryId}");
        assertThat(preDeductLua).contains("ticket:stock:deducted:{requestId}");
        assertThat(preDeductLua).contains("redis.call('EXISTS', deducted_key)");
        assertThat(preDeductLua).contains("redis.call('DECRBY', stock_key");
        assertThat(releaseLua).contains("ticket:stock:compensated:{requestId}");
        assertThat(releaseLua).contains("redis.call('INCRBY', stock_key");
        assertThat(releaseLua).contains("redis.call('DEL', deducted_key)");
    }

    @Test
    void porterAssetsContainAtomicMoveLockAndVersionedBucketSql() throws Exception {
        String porterLua = Files.readString(Path.of("src/main/resources/lua/stock_bucket_porter_move.lua"));
        String lockReleaseLua = Files.readString(Path.of("src/main/resources/lua/redis_lock_release.lua"));
        String stockBucketProperties = Files.readString(Path.of("src/main/java/com/zewbby/smartticket/config/StockBucketProperties.java"));
        String bucketXml = Files.readString(Path.of("src/main/resources/mapper/TicketStockBucketMapper.xml"));
        String orderRequestXml = Files.readString(Path.of("src/main/resources/mapper/OrderRequestMapper.xml"));

        assertThat(porterLua).contains("DECRBY");
        assertThat(porterLua).contains("INCRBY");
        assertThat(porterLua).contains("idempotency key");
        assertThat(lockReleaseLua).contains("redis.call('GET', KEYS[1]) == ARGV[1]");
        assertThat(stockBucketProperties).contains("porterEnabled");
        assertThat(stockBucketProperties).contains("porterMoveRecordTtlSeconds");
        assertThat(bucketXml).contains("bucket_version");
        assertThat(orderRequestXml).contains("stock_bucket_version");
    }

    @Test
    void localMessageSqlContainsReliableOutboxStatusesAndConfirmTimeoutIndex() throws Exception {
        String schema = Files.readString(Path.of("docs/sql/schema.sql"));
        String repair = Files.readString(Path.of("docs/sql/local-schema-repair.sql"));
        String localMessageXml = Files.readString(Path.of("src/main/resources/mapper/LocalMessageMapper.xml"));
        String businessTypeEnum = Files.readString(Path.of("src/main/java/com/zewbby/smartticket/enums/LocalMessageBusinessTypeEnum.java"));
        String timeoutProducer = Files.readString(Path.of("src/main/java/com/zewbby/smartticket/mq/OrderTimeoutProducer.java"));
        String rocketMqAsyncConsumer = Files.readString(Path.of("src/main/java/com/zewbby/smartticket/mq/RocketMqAsyncCreateOrderConsumer.java"));

        assertThat(schema).contains("confirmed_at DATETIME NULL");
        assertThat(schema).doesNotContain("returned_at DATETIME NULL");
        assertThat(repair).doesNotContain("add_column_if_missing('local_message', 'returned_at'");
        assertThat(schema).contains("dead_at DATETIME NULL");
        assertThat(schema).contains("KEY idx_status_updated_at (status, updated_at)");
        assertThat(schema).contains("KEY idx_local_message_publish_scan (status, next_retry_time, created_at)");
        assertThat(repair).contains("CALL add_index_if_missing('local_message', 'idx_local_message_publish_scan', "
                + "'KEY `idx_local_message_publish_scan` (`status`, `next_retry_time`, `created_at`)');");
        assertThat(localMessageXml).contains("status IN ('INIT', 'FAILED')");
        assertThat(localMessageXml).doesNotContain("<select id=\"selectPublishableMessages\"");
        assertThat(localMessageXml).contains("<select id=\"selectPublishableMessagesForUpdate\"");
        assertThat(localMessageXml).contains("FOR UPDATE SKIP LOCKED");
        assertThat(localMessageXml).contains("<update id=\"markSendingBatch\"");
        assertThat(localMessageXml).contains("<update id=\"tryMarkSending\">");
        assertThat(localMessageXml).contains("status = 'SENDING'");
        assertThat(localMessageXml).contains("status = 'CONFIRMED'");
        assertThat(localMessageXml).contains("status IN ('SENDING', 'SENT')");
        assertThat(localMessageXml).contains("retry_count = retry_count + 1");
        assertThat(localMessageXml).doesNotContain("<update id=\"markPublishFailedByMessageId\">");
        assertThat(localMessageXml).containsPattern(
                "(?s)<update id=\"markDeadByMessageId\">.*?AND status IN \\('INIT', 'FAILED'\\).*?</update>"
        );
        assertThat(businessTypeEnum).contains("ORDER_TIMEOUT_CLOSE");
        assertThat(timeoutProducer).contains("OrderTimeoutMessagePublisher");
        assertThat(timeoutProducer).contains("orderTimeoutMessagePublisher.publish(message)");
        assertThat(timeoutProducer).doesNotContain("KafkaTemplate");
        assertThat(rocketMqAsyncConsumer).contains("consumeMode = ConsumeMode.ORDERLY");
        assertThat(rocketMqAsyncConsumer).contains("RocketMqConsumerTuningSupport.apply");
    }

    @Test
    void documentationUsesCurrentAsyncOrderAndPerformanceEntrypoints() throws Exception {
        String readme = Files.readString(Path.of("README.md"));
        String apiReadme = Files.readString(Path.of("docs/api/README.md"));
        String performanceReadme = Files.readString(Path.of("docs/performance/README.md"));

        assertThat(readme).contains("高并发购票主链路只走 `POST /api/orders/async`");
        assertThat(readme).contains("`POST /api/orders` 已废弃");
        assertThat(apiReadme).contains("`POST /api/orders/async`");
        assertThat(apiReadme).contains("`POST /api/orders`").contains("已废弃");
        assertThat(performanceReadme).contains("POST /api/orders/async");
    }

    @Test
    void deadLetterSqlContainsConsumerFailureTableAndManualHandlingStatuses() throws Exception {
        String schema = Files.readString(Path.of("docs/sql/schema.sql"));
        String deadLetterXml = Files.readString(Path.of("src/main/resources/mapper/DeadLetterMessageMapper.xml"));

        assertThat(schema).contains("CREATE TABLE dead_letter_message");
        assertThat(schema).contains("exception_type VARCHAR(64) NOT NULL");
        assertThat(schema).contains("KEY idx_dead_letter_business_key (business_key)");
        assertThat(deadLetterXml).contains("<update id=\"markRetried\">");
        assertThat(deadLetterXml).contains("status = 'RETRIED'");
        assertThat(deadLetterXml).contains("<update id=\"markIgnored\">");
        assertThat(deadLetterXml).contains("status = 'IGNORED'");
        assertThat(deadLetterXml).contains("<update id=\"markResolved\">");
        assertThat(deadLetterXml).contains("status = 'RESOLVED'");
    }

    @Test
    void stockConsistencySqlContainsRecordsCompensationAndCasLua() throws Exception {
        String schema = Files.readString(Path.of("docs/sql/schema.sql"));
        String consistencyXml = Files.readString(Path.of("src/main/resources/mapper/StockConsistencyRecordMapper.xml"));
        String compensationXml = Files.readString(Path.of("src/main/resources/mapper/StockCompensationRecordMapper.xml"));
        String repairLua = Files.readString(Path.of("src/main/resources/lua/stock_repair_cas_delta.lua"));

        assertThat(schema).contains("CREATE TABLE stock_consistency_record");
        assertThat(schema).contains("CREATE TABLE stock_compensation_record");
        assertThat(schema).contains("expected_redis_available_stock INT NOT NULL");
        assertThat(consistencyXml).contains("<update id=\"markRepaired\">");
        assertThat(compensationXml).contains("INSERT INTO stock_compensation_record");
        assertThat(repairLua).contains("CONCURRENT_MODIFIED");
        assertThat(repairLua).contains("redis.call('INCRBY', stock_key, delta)");
    }
}
