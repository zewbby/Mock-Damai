package com.zewbby.smartticket.mq;

import com.zewbby.smartticket.aop.MqConsumeTrace;
import com.zewbby.smartticket.service.OrderService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
@ConditionalOnProperty(prefix = "smart-ticket.order-timeout", name = "delay-message-enabled", havingValue = "true")
@ConditionalOnExpression("'${smart-ticket.order-timeout.publisher-mode:kafka}'.equalsIgnoreCase('kafka') || ('${smart-ticket.order-timeout.publisher-mode:kafka}'.equalsIgnoreCase('outbox') && ${smart-ticket.local-message.sender-enabled:true})")
public class OrderTimeoutConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(OrderTimeoutConsumer.class);

    private final OrderService orderService;

    public OrderTimeoutConsumer(OrderService orderService) {
        this.orderService = orderService;
    }

    @KafkaListener(
            topics = "#{@orderTimeoutProperties.kafkaOrderTimeoutTopic}",
            groupId = "#{@orderTimeoutProperties.kafkaOrderTimeoutConsumerGroup}"
    )
    @MqConsumeTrace(
            topic = "order-timeout",
            consumerGroup = "kafka-order-timeout",
            messageId = "#p0?.messageId",
            businessKey = "#p0?.orderId"
    )
    public void consume(OrderTimeoutMessage message) {
        /*
         * Kafka 没有原生延时队列语义。这里的事件只作为触发信号，必须重新校验 expireTime。
         * 事件可能提前到、晚到或重复到；真正的兜底仍是 OrderTimeoutScanTask。
         * Producer callback 只说明 Broker 收到消息，不说明关闭订单成功。
         * 因此消费者必须重新查数据库状态：PAID/CANCELLED/CLOSED 都要跳过，只有 PENDING_PAYMENT 才允许关闭。
         * closeTimeoutOrder 内部使用条件更新和库存 locked_stock 回滚，保证重复消费不会重复释放库存。
         */
        if (message == null) {
            LOGGER.warn("Ignored empty order timeout Kafka message");
            return;
        }
        if (message.getExpireTime() != null && message.getExpireTime().isAfter(LocalDateTime.now())) {
            LOGGER.info("Skipped early order timeout Kafka message, orderId={}, expireTime={}, traceId={}",
                    message.getOrderId(), message.getExpireTime(), message.getTraceId());
            return;
        }
        LOGGER.info("Received order timeout close message, orderId={}, orderNo={}, traceId={}",
                message.getOrderId(), message.getOrderNo(), message.getTraceId());
        orderService.closeTimeoutOrder(message.getOrderId());
    }
}
