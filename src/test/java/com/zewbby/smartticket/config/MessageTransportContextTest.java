package com.zewbby.smartticket.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zewbby.smartticket.mq.*;
import com.zewbby.smartticket.service.AsyncOrderPartitionService;
import com.zewbby.smartticket.service.DeadLetterMessageService;
import com.zewbby.smartticket.service.LocalMessageService;
import com.zewbby.smartticket.service.OrderService;
import com.zewbby.smartticket.task.LocalMessagePublishTask;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.AbstractKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class MessageTransportContextTest {

    // 使用真实 Spring 条件解析、构造器注入和 Kafka listener 注册；不连接 Broker。
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(KafkaAutoConfiguration.class))
            .withUserConfiguration(TransportConfiguration.class)
            .withBean("asyncOrderSubmitProperties", AsyncOrderSubmitProperties.class, AsyncOrderSubmitProperties::new)
            .withBean("orderTimeoutProperties", OrderTimeoutProperties.class, OrderTimeoutProperties::new)
            .withBean("localMessageProperties", LocalMessageProperties.class, LocalMessageProperties::new)
            .withBean("mqConsumerProperties", MqConsumerProperties.class, MqConsumerProperties::new)
            .withBean(AsyncCreateOrderBatchDispatcher.class, () -> mock(AsyncCreateOrderBatchDispatcher.class))
            .withBean(DeadLetterMessageService.class, () -> mock(DeadLetterMessageService.class))
            .withBean(LocalMessageService.class, () -> mock(LocalMessageService.class))
            .withBean(OrderService.class, () -> mock(OrderService.class))
            .withBean(ObjectMapper.class, ObjectMapper::new)
            .withPropertyValues(
                    "spring.kafka.admin.auto-create=false",
                    "spring.kafka.listener.auto-startup=false",
                    "smart-ticket.order-timeout.delay-message-enabled=true"
            );

    @Test
    void flashSaleCreatesRocketMqConsumersWithoutKafkaCommandListeners() {
        runner.withInitializer(context -> context.getEnvironment().setActiveProfiles("flash-sale"))
                .withPropertyValues(
                        "smart-ticket.async-order-submit.publisher-mode=rocketmq",
                        "smart-ticket.async-order-submit.rocket-mq-transaction-message-enabled=true",
                        "smart-ticket.async-order-submit.persist-request-before-publish=false",
                        "smart-ticket.async-order-submit.max-in-flight-per-ticket-category=100000",
                        "smart-ticket.order-timeout.publisher-mode=rocketmq"
                ).run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(RocketMqAsyncCreateOrderConsumer.class);
                    assertThat(context).hasSingleBean(RocketMqOrderTimeoutConsumer.class);
                    assertThat(context).doesNotHaveBean(KafkaAsyncCreateOrderConsumer.class);
                    assertThat(context).doesNotHaveBean(KafkaAsyncCreateOrderDeadLetterConsumer.class);
                    assertThat(context).doesNotHaveBean(OrderTimeoutConsumer.class);
                    assertThat(context).doesNotHaveBean("asyncOrderKafkaListenerContainerFactory");
                    assertThat(context).doesNotHaveBean("asyncCreateOrderTopic");
                    assertThat(context.getBean(KafkaListenerEndpointRegistry.class).getListenerContainers()).isEmpty();
                });
    }

    @Test
    void directKafkaCreatesCommandListenersEvenWhenOutboxSenderIsDisabled() {
        assertKafkaTransport("kafka", false);
    }

    @Test
    void activeOutboxCreatesKafkaCommandListenersAndSender() {
        assertKafkaTransport("outbox", true);
    }

    @Test
    void testOutboxDoesNotCreateKafkaCommandListeners() {
        runner.withPropertyValues(
                "smart-ticket.async-order-submit.publisher-mode=outbox",
                "smart-ticket.order-timeout.publisher-mode=outbox",
                "smart-ticket.local-message.sender-enabled=false"
        ).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean(KafkaAsyncCreateOrderConsumer.class);
            assertThat(context).doesNotHaveBean(KafkaAsyncCreateOrderDeadLetterConsumer.class);
            assertThat(context).doesNotHaveBean(RocketMqAsyncCreateOrderConsumer.class);
            assertThat(context).doesNotHaveBean(OrderTimeoutConsumer.class);
            assertThat(context).doesNotHaveBean("asyncOrderKafkaTemplate");
            assertThat(context).doesNotHaveBean("asyncOrderKafkaListenerContainerFactory");
            assertThat(context).doesNotHaveBean("asyncCreateOrderTopic");
            assertThat(context).doesNotHaveBean("asyncCreateOrderDeadLetterTopic");
            assertThat(context).doesNotHaveBean("orderTimeoutTopic");
            assertThat(context.getBean(KafkaListenerEndpointRegistry.class).getListenerContainers()).isEmpty();
            assertThat(context.getBean(LocalMessageProperties.class).isSenderEnabled()).isFalse();
        });
    }

    private void assertKafkaTransport(String mode, boolean senderEnabled) {
        runner.withPropertyValues(
                "smart-ticket.async-order-submit.publisher-mode=" + mode,
                "smart-ticket.order-timeout.publisher-mode=" + mode,
                "smart-ticket.local-message.sender-enabled=" + senderEnabled
        ).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(KafkaAsyncCreateOrderConsumer.class);
            assertThat(context).hasSingleBean(KafkaAsyncCreateOrderDeadLetterConsumer.class);
            assertThat(context).hasSingleBean(OrderTimeoutConsumer.class);
            assertThat(context).doesNotHaveBean(RocketMqAsyncCreateOrderConsumer.class);
            assertThat(context).doesNotHaveBean(RocketMqOrderTimeoutConsumer.class);
            assertThat(context).hasBean("asyncOrderKafkaTemplate");
            assertThat(context).hasBean("asyncOrderKafkaListenerContainerFactory");
            assertThat(context).hasBean("asyncCreateOrderTopic");
            assertThat(context).hasBean("asyncCreateOrderDeadLetterTopic");
            assertThat(context).hasBean("orderTimeoutTopic");
            assertThat(context).hasSingleBean(LocalMessagePublishTask.class);
            assertThat(context.getBean(LocalMessageProperties.class).isSenderEnabled()).isEqualTo(senderEnabled);
            assertThat(context.getBean(KafkaListenerEndpointRegistry.class).getListenerContainers()).hasSize(3);
        });
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties
    @Import({KafkaAsyncOrderConfig.class, AsyncOrderSubmitGuardrail.class,
            AsyncOrderPartitionService.class, KafkaAsyncCreateOrderConsumer.class,
            KafkaAsyncCreateOrderDeadLetterConsumer.class, RocketMqAsyncCreateOrderConsumer.class,
            OrderTimeoutConsumer.class, RocketMqOrderTimeoutConsumer.class, LocalMessagePublishTask.class})
    static class TransportConfiguration {
        @Bean
        static BeanPostProcessor disableBrokerConnections() {
            return new BeanPostProcessor() {
                @Override
                public Object postProcessAfterInitialization(Object bean, String beanName) {
                    if (bean instanceof AbstractKafkaListenerContainerFactory<?, ?, ?> factory) {
                        factory.setAutoStartup(false);
                    }
                    return bean;
                }
            };
        }
    }
}
