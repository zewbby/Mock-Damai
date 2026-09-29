package com.zewbby.smartticket.service.impl;

import com.zewbby.smartticket.config.OrderTimeoutProperties;
import com.zewbby.smartticket.mq.OrderTimeoutMessage;
import com.zewbby.smartticket.service.OrderTimeoutMessagePublisher;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.UUID;

@Component
@ConditionalOnProperty(prefix = "smart-ticket.order-timeout", name = "publisher-mode", havingValue = "kafka")
public class KafkaOrderTimeoutMessagePublisher implements OrderTimeoutMessagePublisher {

    private final KafkaTemplate<String, OrderTimeoutMessage> kafkaTemplate;

    private final OrderTimeoutProperties orderTimeoutProperties;

    public KafkaOrderTimeoutMessagePublisher(
            @Qualifier("orderTimeoutKafkaTemplate")
            KafkaTemplate<String, OrderTimeoutMessage> kafkaTemplate,
            OrderTimeoutProperties orderTimeoutProperties) {
        this.kafkaTemplate = kafkaTemplate;
        this.orderTimeoutProperties = orderTimeoutProperties;
    }

    @Override
    public String publish(OrderTimeoutMessage message) {
        String messageId = ensureMessageId(message);
        publishAfterCommit(message);
        return messageId;
    }

    private void publishAfterCommit(OrderTimeoutMessage message) {
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            publishKafka(message);
            return;
        }
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                publishKafka(message);
            }
        });
    }

    private void publishKafka(OrderTimeoutMessage message) {
        kafkaTemplate.send(
                orderTimeoutProperties.getKafkaOrderTimeoutTopic(),
                orderTimeoutKey(message),
                message
        );
    }

    private String ensureMessageId(OrderTimeoutMessage message) {
        if (message.getMessageId() == null || message.getMessageId().isBlank()) {
            message.setMessageId("OT" + UUID.randomUUID().toString().replace("-", ""));
        }
        return message.getMessageId();
    }

    private String orderTimeoutKey(OrderTimeoutMessage message) {
        if (message == null || message.getOrderId() == null) {
            return "order:unknown";
        }
        return "order:" + message.getOrderId();
    }
}
