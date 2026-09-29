package com.zewbby.smartticket.config;

import com.zewbby.smartticket.mq.AsyncCreateOrderMessage;
import com.zewbby.smartticket.mq.OrderTimeoutMessage;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaAsyncOrderConfig {

    @Bean
    @ConditionalOnProperty(prefix = "smart-ticket.async-order-submit", name = "publisher-mode", havingValue = "kafka")
    @SuppressWarnings({"rawtypes", "unchecked"})
    public KafkaTemplate<String, AsyncCreateOrderMessage> asyncOrderKafkaTemplate(ProducerFactory producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    @ConditionalOnProperty(prefix = "smart-ticket.order-timeout", name = "publisher-mode", havingValue = "kafka")
    @SuppressWarnings({"rawtypes", "unchecked"})
    public KafkaTemplate<String, OrderTimeoutMessage> orderTimeoutKafkaTemplate(ProducerFactory producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    @SuppressWarnings({"rawtypes", "unchecked"})
    public KafkaTemplate<String, Object> localMessageKafkaTemplate(ProducerFactory producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    @ConditionalOnProperty(prefix = "smart-ticket.async-order-submit", name = "publisher-mode", havingValue = "kafka")
    public NewTopic asyncCreateOrderTopic(AsyncOrderSubmitProperties asyncOrderSubmitProperties,
                                          MqConsumerProperties mqConsumerProperties) {
        return TopicBuilder.name(asyncOrderSubmitProperties.getKafkaAsyncCreateOrderTopic())
                .partitions(mqConsumerProperties.getAsyncQueueShardCount())
                .build();
    }

    @Bean
    @ConditionalOnProperty(prefix = "smart-ticket.async-order-submit", name = "publisher-mode", havingValue = "kafka")
    public NewTopic asyncCreateOrderDeadLetterTopic(AsyncOrderSubmitProperties asyncOrderSubmitProperties,
                                                    MqConsumerProperties mqConsumerProperties) {
        return TopicBuilder.name(asyncOrderSubmitProperties.getKafkaAsyncCreateOrderDeadLetterTopic())
                .partitions(mqConsumerProperties.getAsyncQueueShardCount())
                .build();
    }

    @Bean
    @ConditionalOnProperty(prefix = "smart-ticket.order-timeout", name = "publisher-mode", havingValue = "kafka")
    public NewTopic orderTimeoutTopic(OrderTimeoutProperties orderTimeoutProperties,
                                      MqConsumerProperties mqConsumerProperties) {
        return TopicBuilder.name(orderTimeoutProperties.getKafkaOrderTimeoutTopic())
                .partitions(mqConsumerProperties.getAsyncQueueShardCount())
                .build();
    }

    @Bean
    @ConditionalOnProperty(prefix = "smart-ticket.async-order-submit", name = "publisher-mode", havingValue = "kafka")
    @SuppressWarnings({"rawtypes", "unchecked"})
    public ConcurrentKafkaListenerContainerFactory<String, AsyncCreateOrderMessage> asyncOrderKafkaListenerContainerFactory(
            ConsumerFactory consumerFactory,
            KafkaTemplate<String, AsyncCreateOrderMessage> asyncOrderKafkaTemplate,
            AsyncOrderSubmitProperties asyncOrderSubmitProperties,
            MqConsumerProperties mqConsumerProperties) {
        ConcurrentKafkaListenerContainerFactory<String, AsyncCreateOrderMessage> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(mqConsumerProperties.getConcurrentConsumers());
        factory.setCommonErrorHandler(asyncOrderKafkaErrorHandler(
                asyncOrderKafkaTemplate,
                asyncOrderSubmitProperties,
                mqConsumerProperties
        ));
        return factory;
    }

    private DefaultErrorHandler asyncOrderKafkaErrorHandler(
            KafkaTemplate<String, AsyncCreateOrderMessage> asyncOrderKafkaTemplate,
            AsyncOrderSubmitProperties asyncOrderSubmitProperties,
            MqConsumerProperties mqConsumerProperties) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                asyncOrderKafkaTemplate,
                (record, exception) -> new TopicPartition(
                        asyncOrderSubmitProperties.getKafkaAsyncCreateOrderDeadLetterTopic(),
                        record.partition()
                )
        );
        long retryIntervalMillis = mqConsumerProperties.getRetryIntervalSeconds() * 1000L;
        long retryTimes = Math.max(0, mqConsumerProperties.getMaxRetryCount() - 1L);
        return new DefaultErrorHandler(recoverer, new FixedBackOff(retryIntervalMillis, retryTimes));
    }
}
