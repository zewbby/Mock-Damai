package com.zewbby.smartticket.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.zewbby.smartticket.mq.AsyncCreateOrderMessage;
import com.zewbby.smartticket.service.AdminBusinessService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class AsyncOrderPipelineIntegrationTest extends BaseIntegrationTest {

    @Autowired
    private AdminBusinessService adminBusinessService;

    @Test
    void asyncOrderOutboxAndConsumerCoreUseRealRedisAndMysql() throws Exception {
        /*
         * 这里验证的是当前集成测试明确拥有的真实边界：
         * HTTP -> Redis Lua 预扣 -> ticket_order_request/local_message -> Consumer Core -> MySQL。
         *
         * Kafka / RocketMQ transport 不在本测试中伪装成真实 Broker；
         * 对应适配器和 Guardrail 由 mq/config 单元测试覆盖。
         */
        adminBusinessService.preheatStock(2L);
        String token = loginAsUser();
        String idempotencyToken = getJson("/api/orders/idempotency-token", token).at("/data/token").asText();

        JsonNode submitResponse = postJson("/api/orders/async", Map.of(
                "showId", 1L,
                "sessionId", 1L,
                "ticketCategoryId", 2L,
                "quantity", 1,
                "idempotencyToken", idempotencyToken
        ), token);
        String requestId = submitResponse.at("/data/requestId").asText();

        assertThat(jdbcTemplate.queryForObject(
                "SELECT COUNT(1) FROM ticket_order_request WHERE request_id = ?",
                Integer.class,
                requestId
        )).isEqualTo(1);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT COUNT(1) FROM local_message WHERE business_type = 'ASYNC_CREATE_ORDER' AND business_key = ?",
                Integer.class,
                requestId
        )).isEqualTo(1);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT available_stock FROM ticket_stock WHERE ticket_category_id = 2",
                Integer.class
        )).isEqualTo(1000);

        String payloadJson = jdbcTemplate.queryForObject(
                "SELECT payload FROM local_message WHERE business_type = 'ASYNC_CREATE_ORDER' AND business_key = ?",
                String.class,
                requestId
        );
        AsyncCreateOrderMessage payload = objectMapper.readValue(payloadJson, AsyncCreateOrderMessage.class);

        assertThat(deliverAsyncCreateOrderOutboxOnce()).isEqualTo(1);

        Long orderId = jdbcTemplate.queryForObject(
                "SELECT order_id FROM ticket_order_request WHERE request_id = ?",
                Long.class,
                requestId
        );
        assertThat(orderId).isNotNull();
        assertThat(jdbcTemplate.queryForObject(
                "SELECT status FROM ticket_order WHERE id = ?",
                String.class,
                orderId
        )).isEqualTo("PENDING_PAYMENT");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT available_stock FROM ticket_stock WHERE ticket_category_id = 2",
                Integer.class
        )).isEqualTo(999);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT locked_stock FROM ticket_stock WHERE ticket_category_id = 2",
                Integer.class
        )).isEqualTo(1);

        // 模拟 Broker 至少一次投递语义：同一业务消息再次到达，不能重复创建正式订单。
        asyncCreateOrderConsumer.consume(payload);

        assertThat(jdbcTemplate.queryForObject(
                "SELECT COUNT(1) FROM ticket_order WHERE ticket_category_id = 2",
                Integer.class
        )).isEqualTo(1);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT available_stock FROM ticket_stock WHERE ticket_category_id = 2",
                Integer.class
        )).isEqualTo(999);
    }
}
