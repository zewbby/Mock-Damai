package com.zewbby.smartticket.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.zewbby.smartticket.service.AdminBusinessService;
import com.zewbby.smartticket.service.OrderService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class PaymentLifecycleIntegrationTest extends BaseIntegrationTest {

    @Autowired
    private AdminBusinessService adminBusinessService;

    @Autowired
    private OrderService orderService;

    @Test
    void paymentSuccessRepeatCallbackCancelAndTimeoutUseRealDatabaseStateTransitions() throws Exception {
        /*
         * 交易系统最怕“状态覆盖”：取消后又支付、支付后又超时关闭、重复回调重复确认库存。
         * 这里用真实 MySQL 事务和 Mapper SQL 验证支付单、订单和库存三张表能一起保持状态机约束。
         */
        adminBusinessService.preheatStock(2L);
        String token = loginAsUser();

        Long paidOrderId = submitAsyncOrderAndWaitSuccess(token, 2L);
        String paymentNo = createPayment(token, paidOrderId);

        JsonNode payResponse = postJson("/api/payments/mock-pay", mockPaymentBody(paymentNo, true), token);
        assertThat(payResponse.at("/data/status").asText()).isEqualTo("SUCCESS");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT status FROM ticket_order WHERE id = ?",
                String.class,
                paidOrderId
        )).isEqualTo("PAID");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT locked_stock FROM ticket_stock WHERE ticket_category_id = 2",
                Integer.class
        )).isZero();
        assertThat(jdbcTemplate.queryForObject(
                "SELECT sold_stock FROM ticket_stock WHERE ticket_category_id = 2",
                Integer.class
        )).isEqualTo(1);

        postJson("/api/payments/mock-pay", mockPaymentBody(paymentNo, true), token);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT sold_stock FROM ticket_stock WHERE ticket_category_id = 2",
                Integer.class
        )).isEqualTo(1);

        orderService.closeTimeoutOrder(paidOrderId);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT status FROM ticket_order WHERE id = ?",
                String.class,
                paidOrderId
        )).isEqualTo("PAID");

        // 同一用户同一票档的提交 Guard 保留 10 秒；用另一票档验证独立的取消链路。
        adminBusinessService.preheatStock(1L);
        Long cancelledOrderId = submitAsyncOrderAndWaitSuccess(token, 1L);
        String cancelledPaymentNo = createPayment(token, cancelledOrderId);
        JsonNode cancelResponse = postJson("/api/orders/" + cancelledOrderId + "/cancel", Map.of(), token);
        assertThat(cancelResponse.at("/data/status").asText()).isEqualTo("CANCELLED");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT status FROM payment_order WHERE payment_no = ?",
                String.class,
                cancelledPaymentNo
        )).isEqualTo("CLOSED");

        JsonNode cancelledPayResponse = postJson("/api/payments/mock-pay", mockPaymentBody(cancelledPaymentNo, true), token);
        assertThat(cancelledPayResponse.at("/code").asInt()).isEqualTo(400);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT status FROM ticket_order WHERE id = ?",
                String.class,
                cancelledOrderId
        )).isEqualTo("CANCELLED");
    }

    private String createPayment(String token, Long orderId) throws Exception {
        JsonNode response = postJson("/api/payments/create", Map.of(
                "orderId", orderId,
                "channel", "MOCK"
        ), token);
        return response.at("/data/paymentNo").asText();
    }
}
