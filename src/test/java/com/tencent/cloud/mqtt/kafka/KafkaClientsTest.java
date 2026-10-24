package com.tencent.cloud.mqtt.kafka;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import com.tencent.cloud.mqtt.model.Connector;

class KafkaClientsTest {
    private static Connector connector(Map<String, Object> properties) {
        Connector connector = new Connector();
        connector.setProperties(properties);
        return connector;
    }

    private static Map<String, Object> properties() {
        return new HashMap<>(Map.of("bootstrap.servers", "localhost:9092"));
    }

    @Test
    void defaultsPreserveNativeTimeoutsAndDoNotMutateConnector() {
        Map<String, Object> input = properties();
        Map<String, Object> built = KafkaClients.producerConfig(connector(input), "sink-task-0");
        assertNotSame(input, built);
        assertEquals(Map.of("bootstrap.servers", "localhost:9092"), input);
        assertEquals("all", built.get("acks"));
        assertEquals(true, built.get("enable.idempotence"));
        assertEquals("sink-task-0", built.get("client.id"));
        assertEquals(StringSerializer.class, built.get("key.serializer"));
        assertEquals(StringSerializer.class, built.get("value.serializer"));
        ProducerConfig nativeConfig = new ProducerConfig(built);
        assertEquals(120_000, nativeConfig.getInt("delivery.timeout.ms"));
        assertEquals(60_000L, nativeConfig.getLong("max.block.ms"));
    }

    @Test
    void nativeOverridesAndSecurityArePreservedWithoutCredentialTranslation() {
        Map<String, Object> input = properties();
        input.putAll(Map.of("acks", "1", "enable.idempotence", false,
            "client.id", "custom-client", "delivery.timeout.ms", 200_000,
            "max.block.ms", 500L, "compression.type", "zstd",
            "security.protocol", "SASL_SSL", "sasl.mechanism", "PLAIN",
            "sasl.jaas.config", "example.LoginModule required;"));
        Connector connector = connector(input);
        connector.setUsername("ignored-user");
        connector.setPassword("ignored-password");
        Map<String, Object> built = KafkaClients.producerConfig(connector, "sink-task-0");
        input.forEach((key, value) -> assertEquals(value, built.get(key)));
        assertFalse(built.containsKey("username"));
        assertFalse(built.containsKey("password"));
    }

    @Test
    void supportsNativeListsAndCopiesTheirStorage() {
        List<String> bootstrap = new ArrayList<>(List.of("localhost:9092", "localhost:9093"));
        List<String> protocols = new ArrayList<>(List.of("TLSv1.3", "TLSv1.2"));
        Map<String, Object> input = properties();
        input.put("bootstrap.servers", bootstrap);
        input.put("ssl.enabled.protocols", protocols);
        input.put("interceptor.classes", List.of());
        Map<String, Object> built = KafkaClients.producerConfig(connector(input), "sink-task-0");
        assertEquals(bootstrap, built.get("bootstrap.servers"));
        assertEquals(protocols, built.get("ssl.enabled.protocols"));
        assertNotSame(bootstrap, built.get("bootstrap.servers"));
        assertNotSame(protocols, built.get("ssl.enabled.protocols"));
        bootstrap.clear();
        assertEquals(List.of("localhost:9092", "localhost:9093"), built.get("bootstrap.servers"));
    }

    @Test
    void supportsNativeScalarTypes() {
        Map<String, Object> input = properties();
        input.putAll(Map.of("linger.ms", 12, "buffer.memory", 33554432L,
            "enable.metrics.push", false, "retries", "7"));
        Map<String, Object> built = KafkaClients.producerConfig(connector(input), "sink-task-0");
        input.forEach((key, value) -> assertEquals(value, built.get(key)));
    }

    @Test
    void requiresNonblankBootstrapStringOrNonemptyStringList() {
        assertThrows(IllegalArgumentException.class,
            () -> KafkaClients.producerConfig(new Connector(), "sink-task-0"));
        for (Object bad : List.of("", " ", ",", "localhost:9092, ", List.of(),
                List.of(""), List.of("localhost:9092", " "), List.of(9092), 9092, true)) {
            Map<String, Object> input = properties();
            input.put("bootstrap.servers", bad);
            IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> KafkaClients.producerConfig(connector(input), "sink-task-0"));
            assertTrue(failure.getMessage().contains("bootstrap.servers"));
        }
    }

    @Test
    void rejectsNullsAndNestedPropertyValuesWithoutEchoingValues() {
        for (Object bad : new Object[] {null, Map.of("secret", "must-not-appear"),
                List.of(Map.of("secret", "must-not-appear")), java.util.Arrays.asList("ok", null)}) {
            Map<String, Object> input = properties();
            input.put("custom.option", bad);
            IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> KafkaClients.producerConfig(connector(input), "sink-task-0"));
            assertFalse(failure.getMessage().contains("must-not-appear"));
        }
    }

    @Test
    void rejectsBridgeOwnedSerializersAndUnsupportedTransactions() {
        for (String key : List.of("key.serializer", "value.serializer", "transactional.id")) {
            Map<String, Object> input = properties();
            input.put(key, "must-not-appear");
            IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> KafkaClients.producerConfig(connector(input), "sink-task-0"));
            assertTrue(failure.getMessage().contains(key));
            assertFalse(failure.getMessage().contains("must-not-appear"));
        }
    }

    @Test
    void rejectsUnacknowledgedDeliveryEvenWithIdempotenceDisabled() {
        for (Object acks : List.of("0", 0, " 0 ")) {
            Map<String, Object> input = properties();
            input.put("acks", acks);
            input.put("enable.idempotence", false);
            assertThrows(IllegalArgumentException.class,
                () -> KafkaClients.producerConfig(connector(input), "sink-task-0"));
        }
    }

    @Test
    void nativeValidationRejectsInvalidValuesAndIdempotenceConflictsAtStartup() {
        for (Map<String, Object> conflict : List.of(Map.<String, Object>of("retries", 0),
                Map.<String, Object>of("acks", "1"),
                Map.<String, Object>of("max.in.flight.requests.per.connection", 6),
                Map.<String, Object>of("compression.type", "invalid"),
                Map.<String, Object>of("max.block.ms", -1))) {
            Map<String, Object> input = properties();
            input.putAll(conflict);
            assertThrows(ConfigException.class,
                () -> KafkaClients.producerConfig(connector(input), "sink-task-0"));
        }
    }
}
