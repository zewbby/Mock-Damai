package com.zewbby.smartticket.config;

import com.zewbby.smartticket.mq.KafkaAsyncCreateOrderConsumer;
import com.zewbby.smartticket.mq.KafkaAsyncCreateOrderDeadLetterConsumer;
import com.zewbby.smartticket.mq.OrderTimeoutConsumer;
import com.zewbby.smartticket.mq.RocketMqOrderTimeoutConsumer;
import com.zewbby.smartticket.task.LocalMessagePublishTask;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

class ApplicationConfigurationContractTest {

    @Test
    void projectIdentityAndLocalExampleStayAlignedWithMockDamai() throws Exception {
        String pom = Files.readString(Path.of("pom.xml"));
        String application = Files.readString(Path.of("src/main/resources/application.yml"));
        String localExample = Files.readString(Path.of("src/main/resources/application-local.example.yml"));

        assertThat(pom).contains("<groupId>com.zewbby</groupId>");
        assertThat(pom).contains("<artifactId>mock-damai</artifactId>");
        assertThat(pom).contains("<name>Mock-Damai</name>");
        assertThat(application).contains("name: mock-damai");
        assertThat(localExample).contains("pool-name: MockDamaiHikariCP");

        assertThat(localExample).doesNotContain("\nsmart-ticket:");
        assertThat(localExample).doesNotContain("\nrocketmq:");
        assertThat(localExample).doesNotContain("\n  kafka:");
    }

    @Test
    void kafkaConsumerDoesNotForceAllMessagesToAsyncOrderType() throws Exception {
        String application = Files.readString(Path.of("src/main/resources/application.yml"));

        assertThat(application).doesNotContain("spring.json.value.default.type");
        assertThat(application).contains("spring.json.trusted.packages: com.zewbby.smartticket.mq");
    }

    @Test
    void kafkaOrderTimeoutConsumerRequiresDelayMessageAndKafkaBackedTransport() {
        ConditionalOnProperty[] conditions =
                OrderTimeoutConsumer.class.getAnnotationsByType(ConditionalOnProperty.class);

        assertThat(Arrays.stream(conditions)).anyMatch(condition ->
                "smart-ticket.order-timeout".equals(condition.prefix())
                        && Arrays.asList(condition.name()).contains("delay-message-enabled")
                        && "true".equals(condition.havingValue()));

        assertKafkaBackedExpression(
                OrderTimeoutConsumer.class.getAnnotation(ConditionalOnExpression.class),
                "smart-ticket.order-timeout.publisher-mode"
        );
    }

    @Test
    void rocketMqOrderTimeoutConsumerRequiresDelayMessageAndRocketMqMode() {
        ConditionalOnProperty[] conditions =
                RocketMqOrderTimeoutConsumer.class.getAnnotationsByType(ConditionalOnProperty.class);

        assertThat(Arrays.stream(conditions)).anyMatch(condition ->
                "smart-ticket.order-timeout".equals(condition.prefix())
                        && Arrays.asList(condition.name()).contains("delay-message-enabled")
                        && "true".equals(condition.havingValue()));

        assertThat(Arrays.stream(conditions)).anyMatch(condition ->
                "smart-ticket.order-timeout".equals(condition.prefix())
                        && Arrays.asList(condition.name()).contains("publisher-mode")
                        && "rocketmq".equals(condition.havingValue()));
    }

    @Test
    void kafkaInfrastructureCoversDirectKafkaAndActiveOutboxTransport() {
        assertKafkaBackedBean("asyncOrderKafkaTemplate", "smart-ticket.async-order-submit.publisher-mode");
        assertKafkaBackedBean("asyncCreateOrderTopic", "smart-ticket.async-order-submit.publisher-mode");
        assertKafkaBackedBean("asyncCreateOrderDeadLetterTopic", "smart-ticket.async-order-submit.publisher-mode");
        assertKafkaBackedBean("asyncOrderKafkaListenerContainerFactory", "smart-ticket.async-order-submit.publisher-mode");
        assertKafkaBackedBean("orderTimeoutTopic", "smart-ticket.order-timeout.publisher-mode");

        assertKafkaBeanMode(
                "orderTimeoutKafkaTemplate",
                "smart-ticket.order-timeout",
                "publisher-mode",
                "kafka"
        );

        assertThat(kafkaConfigMethod("localMessageKafkaTemplate")
                .getAnnotation(ConditionalOnProperty.class)).isNull();
        assertThat(kafkaConfigMethod("localMessageKafkaTemplate")
                .getAnnotation(ConditionalOnExpression.class)).isNull();

        assertKafkaBackedExpression(
                KafkaAsyncCreateOrderConsumer.class.getAnnotation(ConditionalOnExpression.class),
                "smart-ticket.async-order-submit.publisher-mode"
        );
        assertKafkaBackedExpression(
                KafkaAsyncCreateOrderDeadLetterConsumer.class.getAnnotation(ConditionalOnExpression.class),
                "smart-ticket.async-order-submit.publisher-mode"
        );
    }

    private void assertKafkaBackedBean(String methodName, String modeProperty) {
        assertKafkaBackedExpression(
                kafkaConfigMethod(methodName).getAnnotation(ConditionalOnExpression.class),
                modeProperty
        );
    }

    private void assertKafkaBackedExpression(ConditionalOnExpression condition, String modeProperty) {
        assertThat(condition).isNotNull();
        assertThat(condition.value()).contains(modeProperty);
        assertThat(condition.value()).contains("kafka");
        assertThat(condition.value()).contains("outbox");
        assertThat(condition.value()).contains("smart-ticket.local-message.sender-enabled");
    }

    private void assertKafkaBeanMode(String methodName,
                                     String prefix,
                                     String name,
                                     String havingValue) {
        ConditionalOnProperty condition =
                kafkaConfigMethod(methodName).getAnnotation(ConditionalOnProperty.class);

        assertThat(condition).isNotNull();
        assertThat(condition.prefix()).isEqualTo(prefix);
        assertThat(Arrays.asList(condition.name())).contains(name);
        assertThat(condition.havingValue()).isEqualTo(havingValue);
    }

    private java.lang.reflect.Method kafkaConfigMethod(String methodName) {
        return Arrays.stream(KafkaAsyncOrderConfig.class.getDeclaredMethods())
                .filter(method -> method.getName().equals(methodName))
                .findFirst()
                .orElseThrow();
    }


    @Test
    void localMessageSchedulesAreConfigurationDriven() throws Exception {
        Scheduled publishSchedule = LocalMessagePublishTask.class
                .getDeclaredMethod("publishPendingMessages")
                .getAnnotation(Scheduled.class);
        Scheduled confirmTimeoutSchedule = LocalMessagePublishTask.class
                .getDeclaredMethod("scanConfirmTimeoutMessages")
                .getAnnotation(Scheduled.class);

        assertThat(publishSchedule.fixedDelayString())
                .isEqualTo("#{@localMessageProperties.publishFixedDelayMillis}");
        assertThat(confirmTimeoutSchedule.fixedDelayString())
                .isEqualTo("#{@localMessageProperties.confirmTimeoutScanFixedDelayMillis}");

        String application = Files.readString(Path.of("src/main/resources/application.yml"));
        assertThat(application).contains(
                "publish-fixed-delay-millis: ${SMART_TICKET_LOCAL_MESSAGE_PUBLISH_FIXED_DELAY_MILLIS:3000}"
        );
        assertThat(application).contains(
                "confirm-timeout-scan-fixed-delay-millis: ${SMART_TICKET_LOCAL_MESSAGE_CONFIRM_SCAN_FIXED_DELAY_MILLIS:10000}"
        );
    }

}
