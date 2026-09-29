package com.zewbby.smartticket.config;

import com.zewbby.smartticket.mq.OrderTimeoutConsumer;
import com.zewbby.smartticket.mq.RocketMqOrderTimeoutConsumer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

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
    void kafkaOrderTimeoutConsumerRequiresDelayMessageAndKafkaMode() {
        ConditionalOnProperty[] conditions =
                OrderTimeoutConsumer.class.getAnnotationsByType(ConditionalOnProperty.class);

        assertThat(Arrays.stream(conditions)).anyMatch(condition ->
                "smart-ticket.order-timeout".equals(condition.prefix())
                        && Arrays.asList(condition.name()).contains("delay-message-enabled")
                        && "true".equals(condition.havingValue()));

        assertThat(Arrays.stream(conditions)).anyMatch(condition ->
                "smart-ticket.order-timeout".equals(condition.prefix())
                        && Arrays.asList(condition.name()).contains("publisher-mode")
                        && "kafka".equals(condition.havingValue()));
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
    void kafkaTypedInfrastructureFollowsOwningPublisherMode() {
        assertKafkaBeanMode(
                "asyncOrderKafkaTemplate",
                "smart-ticket.async-order-submit",
                "publisher-mode",
                "kafka"
        );
        assertKafkaBeanMode(
                "asyncCreateOrderTopic",
                "smart-ticket.async-order-submit",
                "publisher-mode",
                "kafka"
        );
        assertKafkaBeanMode(
                "asyncCreateOrderDeadLetterTopic",
                "smart-ticket.async-order-submit",
                "publisher-mode",
                "kafka"
        );
        assertKafkaBeanMode(
                "asyncOrderKafkaListenerContainerFactory",
                "smart-ticket.async-order-submit",
                "publisher-mode",
                "kafka"
        );
        assertKafkaBeanMode(
                "orderTimeoutKafkaTemplate",
                "smart-ticket.order-timeout",
                "publisher-mode",
                "kafka"
        );
        assertKafkaBeanMode(
                "orderTimeoutTopic",
                "smart-ticket.order-timeout",
                "publisher-mode",
                "kafka"
        );

        assertThat(kafkaConfigMethod("localMessageKafkaTemplate")
                .getAnnotation(ConditionalOnProperty.class)).isNull();
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

}
