package com.zewbby.smartticket.mq;

import com.zewbby.smartticket.aop.MqConsumeTrace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnExpression("'${smart-ticket.async-order-submit.publisher-mode:outbox}'.equalsIgnoreCase('kafka') || ('${smart-ticket.async-order-submit.publisher-mode:outbox}'.equalsIgnoreCase('outbox') && ${smart-ticket.local-message.sender-enabled:true})")
public class KafkaAsyncCreateOrderConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAsyncCreateOrderConsumer.class);

    private final AsyncCreateOrderBatchDispatcher asyncCreateOrderBatchDispatcher;

    public KafkaAsyncCreateOrderConsumer(AsyncCreateOrderConsumer asyncCreateOrderConsumer) {
        this(new AsyncCreateOrderBatchDispatcher(asyncCreateOrderConsumer));
    }

    @Autowired
    public KafkaAsyncCreateOrderConsumer(AsyncCreateOrderBatchDispatcher asyncCreateOrderBatchDispatcher) {
        this.asyncCreateOrderBatchDispatcher = asyncCreateOrderBatchDispatcher;
    }

    @KafkaListener(
            topics = "#{@asyncOrderSubmitProperties.kafkaAsyncCreateOrderTopic}",
            groupId = "#{@asyncOrderSubmitProperties.kafkaAsyncCreateOrderConsumerGroup}",
            containerFactory = "asyncOrderKafkaListenerContainerFactory"
    )
    @MqConsumeTrace(
            topic = "async-create-order",
            consumerGroup = "kafka-async-create-order",
            messageId = "#p0?.messageId",
            businessKey = "#p0?.requestId"
    )
    public void consume(AsyncCreateOrderMessage message) {
        if (message == null) {
            LOGGER.warn("Ignored empty Kafka async create order message");
            return;
        }
        LOGGER.debug("Received Kafka async create order message, requestId={}, messageId={}",
                message.getRequestId(), message.getMessageId());
        asyncCreateOrderBatchDispatcher.consume(message);
    }
}
