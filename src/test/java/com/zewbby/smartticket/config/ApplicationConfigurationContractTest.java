package com.zewbby.smartticket.config;

import com.zewbby.smartticket.mq.OrderTimeoutConsumer;
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
}
