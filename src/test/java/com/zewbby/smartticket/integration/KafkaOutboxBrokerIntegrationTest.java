package com.zewbby.smartticket.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zewbby.smartticket.mq.AsyncCreateOrderBatchDispatcher;
import com.zewbby.smartticket.mq.AsyncCreateOrderMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Duration;
import java.net.URI;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;

/**
 * 显式 opt-in 的真实 HTTP -> MySQL/Redis -> Outbox -> Kafka -> Consumer 验收。
 * 必须提供独立、可重建的 MySQL 数据库和 Redis；每项测试会重建表并清空 Redis DB。
 */
@EnabledIfSystemProperty(named = "smartticket.broker-it", matches = "true")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "smart-ticket.async-order-submit.publisher-mode=outbox",
        "smart-ticket.local-message.sender-enabled=true",
        "smart-ticket.local-message.publish-fixed-delay-millis=100",
        "smart-ticket.mq-consumer.concurrent-consumers=1",
        "smart-ticket.mq-consumer.async-queue-shard-count=2",
        "smart-ticket.mq-consumer.async-order-batch-enabled=false",
        "spring.kafka.consumer.auto-offset-reset=earliest"
})
@ActiveProfiles("test")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class KafkaOutboxBrokerIntegrationTest {

    private static final String TOPIC = "mock-damai.broker-it." + UUID.randomUUID();

    @DynamicPropertySource
    static void kafkaIsolation(DynamicPropertyRegistry registry) {
        String mysqlUrl = requiredProperty("mysql-url");
        String database = URI.create(mysqlUrl.substring("jdbc:".length())).getPath();
        if (database == null || !database.endsWith("_broker_it")) {
            throw new IllegalArgumentException("Broker 验收数据库名必须以 _broker_it 结尾，禁止使用开发数据库");
        }
        String redisPort = requiredProperty("redis-port");
        String kafkaServers = requiredProperty("kafka-servers");
        registry.add("spring.datasource.url", () -> mysqlUrl);
        registry.add("spring.datasource.username", () -> System.getProperty("smartticket.broker-it.mysql-user", "root"));
        registry.add("spring.datasource.password", () -> System.getProperty("smartticket.broker-it.mysql-password", ""));
        registry.add("spring.data.redis.host", () -> "127.0.0.1");
        registry.add("spring.data.redis.port", () -> redisPort);
        registry.add("spring.data.redis.password", () -> "");
        registry.add("spring.data.redis.database", () -> 0);
        registry.add("spring.kafka.bootstrap-servers", () -> kafkaServers);
        registry.add("smart-ticket.async-order-submit.kafka-async-create-order-topic", () -> TOPIC);
        registry.add("smart-ticket.async-order-submit.kafka-async-create-order-dead-letter-topic", () -> TOPIC + ".DLT");
        registry.add("smart-ticket.async-order-submit.kafka-async-create-order-consumer-group", () -> TOPIC);
    }

    private static String requiredProperty(String name) {
        String value = System.getProperty("smartticket.broker-it." + name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("必须显式指定独立验收基础设施: smartticket.broker-it." + name);
        }
        return value;
    }

    @Autowired private TestRestTemplate http;
    @Autowired private JdbcTemplate jdbc;
    @Autowired private ObjectMapper mapper;
    @Autowired private StringRedisTemplate redis;
    @Autowired @Qualifier("localMessageKafkaTemplate") private KafkaTemplate<String, Object> kafka;
    @MockitoSpyBean private AsyncCreateOrderBatchDispatcher dispatcher;

    @AfterEach
    void clearIsolatedRedisDatabase() {
        try (var connection = redis.getConnectionFactory().getConnection()) {
            connection.serverCommands().flushDb();
        }
    }

    @Test
    void httpSubmitTravelsThroughOutboxAndKafkaAndDuplicateDeliveryDoesNotDeductAgain() throws Exception {
        String adminToken = request(HttpMethod.POST, "/api/auth/login", Map.of(
                "phone", "13800000002", "password", "Test123456"
        ), null, 200).at("/data/token").asText();
        request(HttpMethod.POST, "/api/admin/ticket-categories/2/stock/preheat", null, adminToken, 0);
        String token = request(HttpMethod.POST, "/api/auth/login", Map.of(
                "phone", "13800000001", "password", "Test123456"
        ), null, 200).at("/data/token").asText();
        assertThat(token).isNotBlank();
        String idempotencyToken = request(HttpMethod.GET, "/api/orders/idempotency-token", null, token, 0)
                .at("/data/token").asText();
        JsonNode response = request(HttpMethod.POST, "/api/orders/async", Map.of(
                "showId", 1L, "sessionId", 1L, "ticketCategoryId", 2L,
                "quantity", 1, "idempotencyToken", idempotencyToken
        ), token, 0);
        String requestId = response.at("/data/requestId").asText();
        assertThat(requestId).isNotBlank();

        await("Outbox Kafka confirmed and order created", () ->
                count("SELECT COUNT(*) FROM ticket_order_request WHERE request_id=? AND status='SUCCESS'", requestId) == 1
                && count("SELECT COUNT(*) FROM local_message WHERE business_key=? AND status='CONFIRMED'", requestId) == 1);
        assertThat(count("SELECT COUNT(*) FROM ticket_order WHERE ticket_category_id=2 AND status='PENDING_PAYMENT'"))
                .isEqualTo(1);
        assertStock(999, 1);
        String payload = jdbc.queryForObject("SELECT payload FROM local_message WHERE business_key=?", String.class, requestId);
        AsyncCreateOrderMessage message = mapper.readValue(payload, AsyncCreateOrderMessage.class);
        CompletableFuture<Void> duplicateProcessed = new CompletableFuture<>();
        doAnswer(invocation -> {
            Object result = invocation.callRealMethod();
            duplicateProcessed.complete(null);
            return result;
        }).when(dispatcher).consume(argThat(value -> requestId.equals(value.getRequestId())));
        kafka.send(TOPIC, "ticket:2", message).get();
        duplicateProcessed.get(10, TimeUnit.SECONDS);
        assertThat(count("SELECT COUNT(*) FROM ticket_order WHERE ticket_category_id=2")).isEqualTo(1);
        assertStock(999, 1);
    }

    @Test
    void listenerFailureRetriesThroughKafkaDltAndPersistsDeadLetterMetadata() throws Exception {
        String requestId = "poison-" + UUID.randomUUID();
        AtomicInteger attempts = new AtomicInteger();
        // 只注入消费边界故障；Kafka listener/retry/recoverer/DLT 与数据库全部使用真实实现。
        doAnswer(invocation -> {
            AsyncCreateOrderMessage message = invocation.getArgument(0);
            if (requestId.equals(message.getRequestId())) {
                attempts.incrementAndGet();
                throw new IllegalStateException("broker-it injected consumer failure");
            }
            return invocation.callRealMethod();
        }).when(dispatcher).consume(any(AsyncCreateOrderMessage.class));
        AsyncCreateOrderMessage message = new AsyncCreateOrderMessage(requestId, 1L, 1L, 1L, 2L, 1);
        message.setMessageId("MSG-" + requestId);
        kafka.send(TOPIC, "ticket:2", message).get();
        await("Kafka retry exhausted -> DLT -> dead_letter_message", () ->
                count("SELECT COUNT(*) FROM dead_letter_message WHERE message_id=?", message.getMessageId()) == 1);
        assertThat(attempts.get()).isEqualTo(2);
        Map<String, Object> deadLetter = jdbc.queryForMap(
                "SELECT queue_name, exchange_name, routing_key FROM dead_letter_message WHERE message_id=?",
                message.getMessageId());
        assertThat(deadLetter).containsEntry("queue_name", TOPIC + ".DLT")
                .containsEntry("exchange_name", TOPIC).containsEntry("routing_key", "ticket:2");
        assertThat(count("SELECT COUNT(*) FROM ticket_order")).isZero();
        assertStock(1000, 0);
    }

    private JsonNode request(HttpMethod method, String path, Object body, String token, int expectedCode) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        if (token != null) headers.setBearerAuth(token);
        ResponseEntity<JsonNode> response = http.exchange(path, method, new HttpEntity<>(body, headers), JsonNode.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().path("code").asInt()).as("HTTP response: %s", response.getBody()).isEqualTo(expectedCode);
        return response.getBody();
    }

    private int count(String sql, Object... args) {
        return jdbc.queryForObject(sql, Integer.class, args);
    }

    private void assertStock(int available, int locked) {
        assertThat(jdbc.queryForObject("SELECT available_stock FROM ticket_stock WHERE ticket_category_id=2", Integer.class))
                .isEqualTo(available);
        assertThat(jdbc.queryForObject("SELECT locked_stock FROM ticket_stock WHERE ticket_category_id=2", Integer.class))
                .isEqualTo(locked);
    }

    private void await(String description, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) return;
            Thread.sleep(100);
        }
        throw new AssertionError("等待超时: " + description);
    }
}
