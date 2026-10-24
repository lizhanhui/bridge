package com.tencent.cloud.mqtt.kafka;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.StringDeserializer;
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

    private static Map<String, Object> consumerProperties() {
        Map<String, Object> input = properties();
        input.put("group.id", "bridge-group");
        input.put("auto.offset.reset", "earliest");
        return input;
    }

    @Test
    void consumerDefaultsPreserveNativeBatchSizeAndDoNotMutateConnector() {
        Map<String, Object> input = consumerProperties();
        Map<String, Object> original = new HashMap<>(input);
        Map<String, Object> built = KafkaClients.consumerConfig(connector(input), "source-task-0");
        assertNotSame(input, built);
        assertEquals(original, input);
        assertEquals(false, built.get("enable.auto.commit"));
        assertEquals("source-task-0", built.get("client.id"));
        assertEquals("read_committed", built.get("isolation.level"));
        assertEquals("earliest", built.get("auto.offset.reset"));
        assertEquals(StringDeserializer.class, built.get("key.deserializer"));
        assertEquals(StringDeserializer.class, built.get("value.deserializer"));
        assertFalse(built.containsKey("max.poll.records"));
        ConsumerConfig nativeConfig = new ConsumerConfig(built);
        assertEquals(500, nativeConfig.getInt("max.poll.records"));
        assertEquals("classic", nativeConfig.getString("group.protocol"));
    }

    @Test
    void createsNativeConsumerWithSourceConfiguration() {
        Consumer<String, String> consumer = KafkaClients.createConsumer(
            connector(consumerProperties()), "source-task-0");
        try {
            assertInstanceOf(KafkaConsumer.class, consumer);
            consumer.subscribe(List.of("input-topic"));
            assertEquals(java.util.Set.of("input-topic"), consumer.subscription());
        } finally {
            if (consumer != null) {
                consumer.close(Duration.ZERO);
            }
        }
    }

    @Test
    void consumerNativeOverridesAndSecurityArePreservedWithoutCredentialTranslation() {
        Map<String, Object> input = consumerProperties();
        input.putAll(Map.of("enable.auto.commit", "false", "client.id", "custom-source",
            "isolation.level", "read_uncommitted", "max.poll.records", 17,
            "max.poll.interval.ms", 400_000, "fetch.min.bytes", 1024,
            "security.protocol", "SASL_SSL", "sasl.mechanism", "PLAIN",
            "sasl.jaas.config", "example.LoginModule required;"));
        Connector connector = connector(input);
        connector.setUsername("ignored-user");
        connector.setPassword("ignored-password");
        Map<String, Object> built = KafkaClients.consumerConfig(connector, "source-task-0");
        input.forEach((key, value) -> assertEquals(value, built.get(key)));
        ConsumerConfig nativeConfig = new ConsumerConfig(built);
        assertEquals(17, nativeConfig.getInt("max.poll.records"));
        assertEquals("read_uncommitted", nativeConfig.getString("isolation.level"));
        assertFalse(built.containsKey("username"));
        assertFalse(built.containsKey("password"));
    }

    @Test
    void consumerRequiresExplicitNonblankStringGroupIdAndOffsetReset() {
        for (String key : List.of("group.id", "auto.offset.reset")) {
            Map<String, Object> missing = consumerProperties();
            missing.remove(key);
            IllegalArgumentException absent = assertThrows(IllegalArgumentException.class,
                () -> KafkaClients.consumerConfig(connector(missing), "source-task-0"));
            assertTrue(absent.getMessage().contains(key));
            for (Object bad : new Object[] {null, "", " ", 123, false, List.of("value")}) {
                Map<String, Object> input = consumerProperties();
                input.put(key, bad);
                IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                    () -> KafkaClients.consumerConfig(connector(input), "source-task-0"));
                assertTrue(failure.getMessage().contains(key));
            }
        }
    }

    @Test
    void consumerSupportsAllNativeOffsetResetPoliciesIncludingDuration() {
        for (String policy : List.of("earliest", "latest", "none", "by_duration:PT1H", "by_duration:PT0S")) {
            Map<String, Object> input = consumerProperties();
            input.put("auto.offset.reset", policy);
            Map<String, Object> built = KafkaClients.consumerConfig(connector(input), "source-task-0");
            assertEquals(policy, built.get("auto.offset.reset"));
        }
    }

    @Test
    void consumerNativelyRejectsInvalidOffsetResetPoliciesAndTuning() {
        for (String policy : List.of("invalid", "by_duration", "by_duration:", "by_duration:-PT1H")) {
            Map<String, Object> input = consumerProperties();
            input.put("auto.offset.reset", policy);
            assertThrows(ConfigException.class,
                () -> KafkaClients.consumerConfig(connector(input), "source-task-0"));
        }
        for (Map<String, Object> invalid : List.of(Map.<String, Object>of("max.poll.records", 0),
                Map.<String, Object>of("max.poll.interval.ms", -1),
                Map.<String, Object>of("isolation.level", "invalid"),
                Map.<String, Object>of("enable.auto.commit", "invalid"))) {
            Map<String, Object> input = consumerProperties();
            input.putAll(invalid);
            assertThrows(ConfigException.class,
                () -> KafkaClients.consumerConfig(connector(input), "source-task-0"));
        }
    }

    @Test
    void consumerRejectsAutoCommitRatherThanOverridingIt() {
        for (Object enabled : List.of(true, "true", " TRUE ")) {
            Map<String, Object> input = consumerProperties();
            input.put("enable.auto.commit", enabled);
            IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> KafkaClients.consumerConfig(connector(input), "source-task-0"));
            assertTrue(failure.getMessage().contains("enable.auto.commit"));
            assertEquals(enabled, input.get("enable.auto.commit"));
        }
    }

    @Test
    void consumerRejectsSourceOwnedDeserializersWithoutEchoingValues() {
        for (String key : List.of("key.deserializer", "value.deserializer")) {
            Map<String, Object> input = consumerProperties();
            input.put(key, "must-not-appear");
            IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> KafkaClients.consumerConfig(connector(input), "source-task-0"));
            assertTrue(failure.getMessage().contains(key));
            assertFalse(failure.getMessage().contains("must-not-appear"));
        }
    }

    @Test
    void consumerOnlySupportsNativeClassicProtocolCaseInsensitively() {
        for (String protocol : List.of("classic", "CLASSIC", "Classic")) {
            Map<String, Object> input = consumerProperties();
            input.put("group.protocol", protocol);
            assertEquals(protocol, KafkaClients.consumerConfig(connector(input), "source-task-0")
                .get("group.protocol"));
        }
        Map<String, Object> input = consumerProperties();
        input.put("group.protocol", "consumer");
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
            () -> KafkaClients.consumerConfig(connector(input), "source-task-0"));
        assertTrue(failure.getMessage().contains("group.protocol"));
        assertTrue(failure.getMessage().contains("classic"));
    }

    @Test
    void consumerRejectsStaticGroupIdentityPresence() {
        for (Object identity : new Object[] {"lane-identity", "", null}) {
            Map<String, Object> input = consumerProperties();
            input.put("group.instance.id", identity);
            IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> KafkaClients.consumerConfig(connector(input), "source-task-0"));
            assertTrue(failure.getMessage().contains("group.instance.id"));
            assertFalse(failure.getMessage().contains("lane-identity"));
        }
    }

    @Test
    void consumerRejectsNonemptyInterceptorsAndAcceptsNativeEmptyForms() {
        for (Object interceptors : List.of("must-not-appear", List.of("must-not-appear"))) {
            Map<String, Object> input = consumerProperties();
            input.put("interceptor.classes", interceptors);
            IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> KafkaClients.consumerConfig(connector(input), "source-task-0"));
            assertTrue(failure.getMessage().contains("interceptor.classes"));
            assertFalse(failure.getMessage().contains("must-not-appear"));
        }
        for (Object empty : List.of("", " ", List.of())) {
            Map<String, Object> input = consumerProperties();
            input.put("interceptor.classes", empty);
            assertEquals(empty, KafkaClients.consumerConfig(connector(input), "source-task-0")
                .get("interceptor.classes"));
        }
    }

    @Test
    void consumerLeavesOppositeProducerOptionsUnusedAndUnrestricted() {
        Map<String, Object> input = consumerProperties();
        input.putAll(Map.of("key.serializer", "not-a-consumer-class",
            "value.serializer", "not-a-consumer-class", "acks", "0",
            "transactional.id", "unused-producer-identity"));
        Map<String, Object> built = KafkaClients.consumerConfig(connector(input), "source-task-0");
        input.forEach((key, value) -> assertEquals(value, built.get(key)));
    }

    @Test
    void sharedConnectorProducerAndConsumerConfigurationsAreIndependentIncludingLists() {
        List<String> bootstrap = new ArrayList<>(List.of("localhost:9092", "localhost:9093"));
        List<String> protocols = new ArrayList<>(List.of("TLSv1.3", "TLSv1.2"));
        Map<String, Object> input = consumerProperties();
        input.put("bootstrap.servers", bootstrap);
        input.put("ssl.enabled.protocols", protocols);
        Connector connector = connector(input);
        Map<String, Object> producer = KafkaClients.producerConfig(connector, "sink-task-0");
        Map<String, Object> consumer = KafkaClients.consumerConfig(connector, "source-task-0");
        assertEquals("sink-task-0", producer.get("client.id"));
        assertEquals("source-task-0", consumer.get("client.id"));
        assertFalse(producer.containsKey("key.deserializer"));
        assertFalse(consumer.containsKey("key.serializer"));
        for (String key : List.of("bootstrap.servers", "ssl.enabled.protocols")) {
            assertNotSame(input.get(key), producer.get(key));
            assertNotSame(input.get(key), consumer.get(key));
            assertNotSame(producer.get(key), consumer.get(key));
            ((List<?>) producer.get(key)).clear();
            assertEquals(input.get(key), consumer.get(key));
        }
        consumer.put("client.id", "changed-source");
        assertEquals("sink-task-0", producer.get("client.id"));
        assertFalse(input.containsKey("client.id"));
        assertFalse(input.containsKey("enable.auto.commit"));
        assertFalse(input.containsKey("enable.idempotence"));
        assertFalse(input.containsKey("isolation.level"));
    }

    @Test
    void consumerRequiresTheSharedBootstrapContract() {
        assertThrows(IllegalArgumentException.class,
            () -> KafkaClients.consumerConfig(new Connector(), "source-task-0"));
        for (Object bad : List.of("", " ", ",", "localhost:9092, ", List.of(),
                List.of(""), List.of("localhost:9092", " "), List.of(9092), 9092, true)) {
            Map<String, Object> input = consumerProperties();
            input.put("bootstrap.servers", bad);
            IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> KafkaClients.consumerConfig(connector(input), "source-task-0"));
            assertTrue(failure.getMessage().contains("bootstrap.servers"));
        }
    }

    @Test
    void consumerRejectsNullsAndNestedValuesWithoutEchoingValues() {
        for (Object bad : new Object[] {null, Map.of("secret", "must-not-appear"),
                List.of(Map.of("secret", "must-not-appear")), java.util.Arrays.asList("ok", null)}) {
            Map<String, Object> input = consumerProperties();
            input.put("custom.option", bad);
            IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> KafkaClients.consumerConfig(connector(input), "source-task-0"));
            assertFalse(failure.getMessage().contains("must-not-appear"));
        }
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
