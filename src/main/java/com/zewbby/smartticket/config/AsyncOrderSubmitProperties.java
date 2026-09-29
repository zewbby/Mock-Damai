package com.zewbby.smartticket.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "smart-ticket.async-order-submit")
public class AsyncOrderSubmitProperties {

    public static final String PUBLISHER_MODE_OUTBOX = "outbox";

    public static final String PUBLISHER_MODE_REDIS_STREAM = "redis-stream";

    public static final String PUBLISHER_MODE_KAFKA = "kafka";

    public static final String PUBLISHER_MODE_ROCKETMQ = "rocketmq";

    /**
     * 是否在入口发布消息前先写 ticket_order_request。
     *
     * 默认开启，保持可靠 Outbox 兼容链路；高并发活动可以关闭，让消费者根据消息补建请求记录，
     * 从入口链路中移除一次 MySQL insert。
     */
    private boolean persistRequestBeforePublish = true;

    /**
     * 异步下单消息发布模式。
     *
     * outbox: 写 local_message，再由本地消息发送器可靠投递 Kafka；只能在非 flash-sale 场景使用。
     * redis-stream: legacy experimental adapter，代码与测试仍保留，但当前 Guardrail 不允许选择该模式。
     * kafka: 入口直接写 Kafka topic；只能在非 flash-sale 场景使用。
     * rocketmq: 入口写 RocketMQ topic；flash-sale profile 强制使用该模式并开启事务消息。
     *
     * Java 字段默认值保留 outbox，便于未经过外部配置绑定的对象和隔离单元测试使用；
     * 真实应用运行默认值由 application.yml 明确为 rocketmq。
     */
    private String publisherMode = PUBLISHER_MODE_OUTBOX;

    /**
     * 是否启用异步下单 in-flight 退避。
     *
     * 它限制“Redis 已准备进入预扣/MQ，但消费者还没处理到终态”的请求数，防止热点票档把 MQ、消费者和 MySQL
     * 堆到不可恢复的延迟区间。
     */
    private boolean inFlightControlEnabled = true;

    private long maxInFlightPerTicketCategory = 20000L;

    private long inFlightCounterTtlSeconds = 600L;

    private String redisStreamName = "stream:order:async:create";

    private String redisStreamConsumerGroup = "order-create-consumers";

    private String redisStreamConsumerName = "order-create-consumer";

    private int redisStreamReadBatchSize = 64;

    private long redisStreamPollFixedDelayMillis = 50L;

    private long redisStreamBlockMillis = 100L;

    private int redisStreamWorkerThreads = 1;

    private String kafkaAsyncCreateOrderTopic = "smart-ticket.async-order.create";

    private String kafkaAsyncCreateOrderDeadLetterTopic = "smart-ticket.async-order.create.DLT";

    private String kafkaAsyncCreateOrderConsumerGroup = "smart-ticket-async-order-create";

    private String rocketMqAsyncCreateOrderTopic = "smart-ticket.async-order.create";

    private String rocketMqAsyncCreateOrderConsumerGroup = "smart-ticket-async-order-create";

    /**
     * RocketMQ 命令链路是否使用事务消息。
     *
     * 开启后，入口会先发送半消息，再在 RocketMQ 本地事务中执行 Redis 预扣。
     * 这样可以避免“Redis 已扣，但普通消息还没发到 Broker”时进程崩溃造成的无消息、无请求记录中间态。
     */
    private boolean rocketMqTransactionMessageEnabled = true;

    public boolean isPersistRequestBeforePublish() {
        return persistRequestBeforePublish;
    }

    public void setPersistRequestBeforePublish(boolean persistRequestBeforePublish) {
        this.persistRequestBeforePublish = persistRequestBeforePublish;
    }

    public String getPublisherMode() {
        return publisherMode == null ? PUBLISHER_MODE_OUTBOX : publisherMode.trim();
    }

    public void setPublisherMode(String publisherMode) {
        this.publisherMode = publisherMode;
    }

    public boolean isRedisStreamPublisherMode() {
        return PUBLISHER_MODE_REDIS_STREAM.equalsIgnoreCase(getPublisherMode());
    }

    public boolean isKafkaPublisherMode() {
        return PUBLISHER_MODE_KAFKA.equalsIgnoreCase(getPublisherMode());
    }

    public boolean isRocketMqPublisherMode() {
        return PUBLISHER_MODE_ROCKETMQ.equalsIgnoreCase(getPublisherMode());
    }

    public boolean isRocketMqTransactionMessageEnabled() {
        return rocketMqTransactionMessageEnabled;
    }

    public void setRocketMqTransactionMessageEnabled(boolean rocketMqTransactionMessageEnabled) {
        this.rocketMqTransactionMessageEnabled = rocketMqTransactionMessageEnabled;
    }

    public boolean isInFlightControlEnabled() {
        return inFlightControlEnabled;
    }

    public void setInFlightControlEnabled(boolean inFlightControlEnabled) {
        this.inFlightControlEnabled = inFlightControlEnabled;
    }

    public long getMaxInFlightPerTicketCategory() {
        return Math.max(1L, maxInFlightPerTicketCategory);
    }

    public void setMaxInFlightPerTicketCategory(long maxInFlightPerTicketCategory) {
        this.maxInFlightPerTicketCategory = maxInFlightPerTicketCategory;
    }

    public long getInFlightCounterTtlSeconds() {
        return Math.max(30L, inFlightCounterTtlSeconds);
    }

    public void setInFlightCounterTtlSeconds(long inFlightCounterTtlSeconds) {
        this.inFlightCounterTtlSeconds = inFlightCounterTtlSeconds;
    }

    public String getRedisStreamName() {
        return redisStreamName == null || redisStreamName.isBlank()
                ? "stream:order:async:create"
                : redisStreamName.trim();
    }

    public void setRedisStreamName(String redisStreamName) {
        this.redisStreamName = redisStreamName;
    }

    public String getRedisStreamConsumerGroup() {
        return redisStreamConsumerGroup == null || redisStreamConsumerGroup.isBlank()
                ? "order-create-consumers"
                : redisStreamConsumerGroup.trim();
    }

    public void setRedisStreamConsumerGroup(String redisStreamConsumerGroup) {
        this.redisStreamConsumerGroup = redisStreamConsumerGroup;
    }

    public String getRedisStreamConsumerName() {
        return redisStreamConsumerName == null || redisStreamConsumerName.isBlank()
                ? "order-create-consumer"
                : redisStreamConsumerName.trim();
    }

    public void setRedisStreamConsumerName(String redisStreamConsumerName) {
        this.redisStreamConsumerName = redisStreamConsumerName;
    }

    public int getRedisStreamReadBatchSize() {
        return Math.max(1, redisStreamReadBatchSize);
    }

    public void setRedisStreamReadBatchSize(int redisStreamReadBatchSize) {
        this.redisStreamReadBatchSize = redisStreamReadBatchSize;
    }

    public long getRedisStreamPollFixedDelayMillis() {
        return Math.max(10L, redisStreamPollFixedDelayMillis);
    }

    public void setRedisStreamPollFixedDelayMillis(long redisStreamPollFixedDelayMillis) {
        this.redisStreamPollFixedDelayMillis = redisStreamPollFixedDelayMillis;
    }

    public long getRedisStreamBlockMillis() {
        return Math.max(1L, redisStreamBlockMillis);
    }

    public void setRedisStreamBlockMillis(long redisStreamBlockMillis) {
        this.redisStreamBlockMillis = redisStreamBlockMillis;
    }

    public int getRedisStreamWorkerThreads() {
        return Math.max(1, redisStreamWorkerThreads);
    }

    public void setRedisStreamWorkerThreads(int redisStreamWorkerThreads) {
        this.redisStreamWorkerThreads = redisStreamWorkerThreads;
    }

    public String getKafkaAsyncCreateOrderTopic() {
        return kafkaAsyncCreateOrderTopic == null || kafkaAsyncCreateOrderTopic.isBlank()
                ? "smart-ticket.async-order.create"
                : kafkaAsyncCreateOrderTopic.trim();
    }

    public void setKafkaAsyncCreateOrderTopic(String kafkaAsyncCreateOrderTopic) {
        this.kafkaAsyncCreateOrderTopic = kafkaAsyncCreateOrderTopic;
    }

    public String getKafkaAsyncCreateOrderDeadLetterTopic() {
        return kafkaAsyncCreateOrderDeadLetterTopic == null || kafkaAsyncCreateOrderDeadLetterTopic.isBlank()
                ? getKafkaAsyncCreateOrderTopic() + ".DLT"
                : kafkaAsyncCreateOrderDeadLetterTopic.trim();
    }

    public void setKafkaAsyncCreateOrderDeadLetterTopic(String kafkaAsyncCreateOrderDeadLetterTopic) {
        this.kafkaAsyncCreateOrderDeadLetterTopic = kafkaAsyncCreateOrderDeadLetterTopic;
    }

    public String getKafkaAsyncCreateOrderConsumerGroup() {
        return kafkaAsyncCreateOrderConsumerGroup == null || kafkaAsyncCreateOrderConsumerGroup.isBlank()
                ? "smart-ticket-async-order-create"
                : kafkaAsyncCreateOrderConsumerGroup.trim();
    }

    public void setKafkaAsyncCreateOrderConsumerGroup(String kafkaAsyncCreateOrderConsumerGroup) {
        this.kafkaAsyncCreateOrderConsumerGroup = kafkaAsyncCreateOrderConsumerGroup;
    }

    public String getRocketMqAsyncCreateOrderTopic() {
        return rocketMqAsyncCreateOrderTopic == null || rocketMqAsyncCreateOrderTopic.isBlank()
                ? "smart-ticket.async-order.create"
                : rocketMqAsyncCreateOrderTopic.trim();
    }

    public void setRocketMqAsyncCreateOrderTopic(String rocketMqAsyncCreateOrderTopic) {
        this.rocketMqAsyncCreateOrderTopic = rocketMqAsyncCreateOrderTopic;
    }

    public String getRocketMqAsyncCreateOrderConsumerGroup() {
        return rocketMqAsyncCreateOrderConsumerGroup == null || rocketMqAsyncCreateOrderConsumerGroup.isBlank()
                ? "smart-ticket-async-order-create"
                : rocketMqAsyncCreateOrderConsumerGroup.trim();
    }

    public void setRocketMqAsyncCreateOrderConsumerGroup(String rocketMqAsyncCreateOrderConsumerGroup) {
        this.rocketMqAsyncCreateOrderConsumerGroup = rocketMqAsyncCreateOrderConsumerGroup;
    }

}
