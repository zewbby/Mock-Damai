package com.zewbby.smartticket.mq;

import com.zewbby.smartticket.aop.MqConsumeTrace;
import com.zewbby.smartticket.config.MqConsumerProperties;
import com.zewbby.smartticket.service.OrderService;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.spring.annotation.ConsumeMode;
import org.apache.rocketmq.spring.annotation.MessageModel;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQPushConsumerLifecycleListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
@ConditionalOnProperty(prefix = "smart-ticket.order-timeout", name = "delay-message-enabled", havingValue = "true")
@ConditionalOnProperty(prefix = "smart-ticket.order-timeout", name = "publisher-mode", havingValue = "rocketmq")
@RocketMQMessageListener(
        topic = "${smart-ticket.order-timeout.rocket-mq-order-timeout-topic}",
        consumerGroup = "${smart-ticket.order-timeout.rocket-mq-order-timeout-consumer-group}",
        consumeMode = ConsumeMode.CONCURRENTLY,
        messageModel = MessageModel.CLUSTERING,
        consumeThreadNumber = 24,
        consumeThreadMax = 96,
        maxReconsumeTimes = 3
)
public class RocketMqOrderTimeoutConsumer implements RocketMQListener<OrderTimeoutMessage>,
        RocketMQPushConsumerLifecycleListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(RocketMqOrderTimeoutConsumer.class);

    private final OrderService orderService;

    private final MqConsumerProperties mqConsumerProperties;

    public RocketMqOrderTimeoutConsumer(OrderService orderService) {
        this(orderService, new MqConsumerProperties());
    }

    @Autowired
    public RocketMqOrderTimeoutConsumer(OrderService orderService,
                                        MqConsumerProperties mqConsumerProperties) {
        this.orderService = orderService;
        this.mqConsumerProperties = mqConsumerProperties;
    }

    @Override
    public void prepareStart(DefaultMQPushConsumer consumer) {
        RocketMqConsumerTuningSupport.apply(consumer, mqConsumerProperties);
    }

    @Override
    @MqConsumeTrace(
            topic = "order-timeout",
            consumerGroup = "rocketmq-order-timeout",
            messageId = "#p0?.messageId",
            businessKey = "#p0?.orderId"
    )
    public void onMessage(OrderTimeoutMessage message) {
        if (message == null) {
            LOGGER.warn("Ignored empty order timeout RocketMQ message");
            return;
        }
        if (message.getExpireTime() != null && message.getExpireTime().isAfter(LocalDateTime.now())) {
            LOGGER.info("Skipped early order timeout RocketMQ message, orderId={}, expireTime={}, traceId={}",
                    message.getOrderId(), message.getExpireTime(), message.getTraceId());
            return;
        }
        LOGGER.info("Received RocketMQ order timeout close message, orderId={}, orderNo={}, traceId={}",
                message.getOrderId(), message.getOrderNo(), message.getTraceId());
        orderService.closeTimeoutOrder(message.getOrderId());
    }
}
