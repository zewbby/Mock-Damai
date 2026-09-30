package com.zewbby.smartticket.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zewbby.smartticket.domain.entity.LocalMessage;
import com.zewbby.smartticket.enums.LocalMessageBusinessTypeEnum;
import com.zewbby.smartticket.mq.AsyncCreateOrderConsumer;
import com.zewbby.smartticket.mq.AsyncCreateOrderMessage;
import com.zewbby.smartticket.service.LocalMessageService;
import com.zewbby.smartticket.service.PaymentSignatureService;
import org.junit.jupiter.api.AfterEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@AutoConfigureMockMvc
@ActiveProfiles("test")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@Testcontainers(disabledWithoutDocker = true)
@Sql(scripts = {"file:docs/sql/schema.sql", "file:docs/sql/data.sql"})
public abstract class BaseIntegrationTest {

    @Container
    protected static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.0.36")
            .withDatabaseName("smart_ticket_lite")
            .withUsername("smart_ticket")
            .withPassword("smart_ticket");

    @Container
    protected static final GenericContainer<?> REDIS = new GenericContainer<>(
            DockerImageName.parse("redis:7.2-alpine")
    ).withExposedPorts(6379);

    @Autowired
    protected MockMvc mockMvc;

    @Autowired
    protected ObjectMapper objectMapper;

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    @Autowired
    protected StringRedisTemplate stringRedisTemplate;

    @Autowired
    protected LocalMessageService localMessageService;

    @Autowired
    protected AsyncCreateOrderConsumer asyncCreateOrderConsumer;

    @Autowired
    protected PaymentSignatureService paymentSignatureService;

    /**
     * 集成测试只启动真实 MySQL / Redis Testcontainers。
     *
     * Broker transport 不在这里伪装成“真实 Kafka/RocketMQ 集成测试”：
     * test profile 固定使用 Outbox，消息先真实落 local_message，再由测试基类把
     * ASYNC_CREATE_ORDER payload 显式交给共享 AsyncCreateOrderConsumer。
     *
     * 这样仍然覆盖真实 SQL、事务、Redis Lua、Outbox 数据和消费者状态机，
     * 同时不要求开发机额外运行 Kafka / RocketMQ。
     */
    @DynamicPropertySource
    static void registerContainerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", MYSQL::getJdbcUrl);
        registry.add("spring.datasource.username", MYSQL::getUsername);
        registry.add("spring.datasource.password", MYSQL::getPassword);
        registry.add("spring.datasource.driver-class-name", MYSQL::getDriverClassName);

        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
        registry.add("spring.data.redis.password", () -> "");
        registry.add("spring.data.redis.database", () -> 0);
    }

    @AfterEach
    void cleanInfrastructureState() {
        stringRedisTemplate.getConnectionFactory().getConnection().serverCommands().flushDb();
    }

    protected String loginAsUser() throws Exception {
        return login("13800000001", "Test123456");
    }

    protected String loginAsAdmin() throws Exception {
        return login("13800000002", "Test123456");
    }

    protected String login(String phone, String password) throws Exception {
        JsonNode response = postJson("/api/auth/login", Map.of(
                "phone", phone,
                "password", password
        ), null);
        return response.at("/data/token").asText();
    }

    protected JsonNode getJson(String url, String bearerToken) throws Exception {
        MvcResult result = mockMvc.perform(get(url)
                        .header("Authorization", "Bearer " + bearerToken))
                .andExpect(status().isOk())
                .andReturn();
        return objectMapper.readTree(result.getResponse().getContentAsString());
    }

    protected JsonNode postJson(String url, Object body, String bearerToken) throws Exception {
        var builder = post(url)
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(body));
        if (bearerToken != null) {
            builder.header("Authorization", "Bearer " + bearerToken);
        }
        MvcResult result = mockMvc.perform(builder)
                .andExpect(status().isOk())
                .andReturn();
        return objectMapper.readTree(result.getResponse().getContentAsString());
    }

    /**
     * 领取当前 Outbox 中待投递的异步创单命令并交给共享消费者。
     *
     * claimPublishableMessages 会先把消息从 INIT/FAILED 原子地抢占为 SENDING；
     * 消费成功后再标记 CONFIRMED，避免同一个测试后续再次投递旧消息。
     */
    protected int deliverAsyncCreateOrderOutboxOnce() throws Exception {
        List<LocalMessage> messages = localMessageService.claimPublishableMessages(LocalDateTime.now(), 100);
        int delivered = 0;
        for (LocalMessage message : messages) {
            if (!LocalMessageBusinessTypeEnum.ASYNC_CREATE_ORDER.getCode().equals(message.getBusinessType())) {
                continue;
            }
            AsyncCreateOrderMessage payload = objectMapper.readValue(
                    message.getPayload(),
                    AsyncCreateOrderMessage.class
            );
            asyncCreateOrderConsumer.consume(payload);
            localMessageService.markConfirmed(message.getMessageId());
            delivered++;
        }
        return delivered;
    }

    protected void waitUntil(String description, BooleanSupplier condition) {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(100L);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new AssertionError("等待被中断: " + description, exception);
            }
        }
        throw new AssertionError("等待超时: " + description);
    }

    protected Long submitAsyncOrderAndWaitSuccess(String bearerToken, Long ticketCategoryId) throws Exception {
        JsonNode tokenResponse = getJson("/api/orders/idempotency-token", bearerToken);
        String idempotencyToken = tokenResponse.at("/data/token").asText();

        JsonNode submitResponse = postJson("/api/orders/async", Map.of(
                "showId", 1L,
                "sessionId", 1L,
                "ticketCategoryId", ticketCategoryId,
                "quantity", 1,
                "idempotencyToken", idempotencyToken
        ), bearerToken);
        String requestId = submitResponse.at("/data/requestId").asText();

        int delivered = deliverAsyncCreateOrderOutboxOnce();
        if (delivered < 1) {
            throw new AssertionError("没有找到可投递的 ASYNC_CREATE_ORDER Outbox 消息");
        }

        waitUntil("异步下单请求变为 SUCCESS", () -> {
            String status = jdbcTemplate.queryForObject(
                    "SELECT status FROM ticket_order_request WHERE request_id = ?",
                    String.class,
                    requestId
            );
            return "SUCCESS".equals(status);
        });
        return jdbcTemplate.queryForObject(
                "SELECT order_id FROM ticket_order_request WHERE request_id = ?",
                Long.class,
                requestId
        );
    }

    protected Map<String, Object> mockPaymentBody(String paymentNo, boolean success) {
        Long timestamp = System.currentTimeMillis();
        String nonce = "it-" + UUID.randomUUID();
        String signature = paymentSignatureService.sign(paymentNo, success, timestamp, nonce);
        return Map.of(
                "paymentNo", paymentNo,
                "success", success,
                "timestamp", timestamp,
                "nonce", nonce,
                "signature", signature
        );
    }
}
