package com.tencent.cloud.mqtt;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.message.MessageBuilder;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.hivemq.HiveMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.rocketmq.RocketMQContainer;
import org.testcontainers.utility.DockerImageName;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperties;
import com.tencent.cloud.mqtt.kafka.KafkaSink;
import com.tencent.cloud.mqtt.model.Connector;
import com.tencent.cloud.mqtt.model.ConnectorType;
import com.tencent.cloud.mqtt.rocketmq.RocketMQSource;

@Testcontainers
@Timeout(120)
class KafkaBridgeIntegrationTest {

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.7.0");

    @Container
    static final HiveMQContainer HIVEMQ =
        new HiveMQContainer(DockerImageName.parse("hivemq/hivemq-ce:2026.5"));

    @Container
    static final RocketMQContainer ROCKETMQ = new RocketMQContainer("apache/rocketmq:5.3.4");

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String SQL =
        "SELECT payload.device AS device, payload.temp * 2 AS doubled FROM payload WHERE payload.temp > 20";
    private static final Duration RECEIVE_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration NEGATIVE_WINDOW = Duration.ofSeconds(3);
    private static final byte[] CORRELATION_DATA = {0, (byte) 0xff, 42, (byte) 0x80};

    @TempDir
    Path temporaryDirectory;

    private final AtomicReference<Throwable> taskFailure = new AtomicReference<>();
    private final AtomicBoolean shuttingDown = new AtomicBoolean();
    private Thread taskThread;
    private KafkaConsumer<String, String> verifier;
    private Mqtt5BlockingClient mqttPublisher;
    private Producer rocketMqPublisher;

    @Test
    void mqttToKafkaTransformsAndPreservesMetadata() throws Exception {
        String suffix = uniqueSuffix();
        String inputTopic = "it/kafka/" + suffix + "/in";
        String defaultTopic = "it-kafka-" + suffix + "-default";
        String routedTopic = "it-kafka-" + suffix + "-routed";
        createKafkaVerifier(defaultTopic, routedTopic);

        Path config = temporaryDirectory.resolve("mqtt-kafka.json");
        MAPPER.writeValue(config.toFile(), Map.of(
            "connectors", List.of(
                Map.of("id", "mqtt", "type", "MQTT", "access_point",
                    HIVEMQ.getHost() + ":" + HIVEMQ.getMqttPort()),
                Map.of("id", "kafka", "type", "Kafka", "properties", kafkaProperties())),
            "tasks", List.of(Map.of(
                "name", "mqtt-kafka-" + suffix,
                "source", Map.of("connector_id", "mqtt", "topic_filter", inputTopic),
                "sink", Map.of("connector_id", "kafka", "topic", defaultTopic),
                "sql", SQL))));
        startTask(TaskManager.loadTasks(config).getFirst());

        mqttPublisher = MqttClient.builder().useMqttVersion5()
            .identifier("publisher-" + suffix)
            .serverHost(HIVEMQ.getHost()).serverPort(HIVEMQ.getMqttPort())
            .buildBlocking();
        mqttPublisher.connect();

        long beforePublish = System.currentTimeMillis();
        publishMqtt(inputTopic, "cold", 5, "mqtt-filtered", null);
        publishMqtt(inputTopic, "default", 25, "mqtt-default", null);
        publishMqtt(inputTopic, "routed", 30, "mqtt-routed", routedTopic);

        List<ConsumerRecord<String, String>> records = receiveRecords(2);
        long afterReceive = System.currentTimeMillis();
        assertEquals(2, records.size(), "only the two SQL-matching inputs may reach Kafka");
        ConsumerRecord<String, String> defaultRecord = recordOnTopic(records, defaultTopic);
        ConsumerRecord<String, String> routedRecord = recordOnTopic(records, routedTopic);
        assertTransformed(defaultRecord, "default", 50);
        assertTransformed(routedRecord, "routed", 60);
        assertEquals("mqtt-default", defaultRecord.key());
        assertEquals("mqtt-routed", routedRecord.key());
        for (ConsumerRecord<String, String> record : records) {
            assertEquals(TimestampType.CREATE_TIME, record.timestampType());
            assertTrue(record.timestamp() >= beforePublish && record.timestamp() <= afterReceive,
                "MQTT receive timestamp must survive SQL and Kafka publication");
            assertEquals(inputTopic, headerValue(record, "src.mqtt.topic"));
            assertEquals("1", headerValue(record, Task.HOP_COUNT_HEADER));
            assertEquals("1", headerValue(record, "mqtt.qos"));
            assertEquals("false", headerValue(record, "mqtt.retained"));
            assertEquals("false", headerValue(record, "mqtt.duplicate"));
            assertEquals("application/json", headerValue(record, "mqtt.content.type"));
            assertEquals(inputTopic + "/response", headerValue(record, "mqtt.response.topic"));
            assertArrayEquals(CORRELATION_DATA,
                record.headers().lastHeader("mqtt.correlation.data").value());
            assertEquals(List.of("blue", "green"), headerValues(record, "label"));
            assertEquals("mqtt-integration", headerValue(record, "origin"));
            assertEquals(record.key(), headerValue(record, "$__messageId"));
            assertEquals("upstream-client", headerValue(record, "$__publisherClientId"));
            assertNull(record.headers().lastHeader("dst.kafka.topic"), "routing hint must be consumed");
        }
        assertNoMoreRecords();
    }

    @Test
    void rocketMqToKafkaTransformsAndPreservesMetadata() throws Exception {
        String suffix = uniqueSuffix();
        String inputTopic = "it-kafka-" + suffix + "-input";
        String defaultTopic = "it-kafka-" + suffix + "-default";
        String routedTopic = "it-kafka-" + suffix + "-routed";
        createRocketMqTopic(inputTopic);
        createKafkaVerifier(defaultTopic, routedTopic);

        Connector sourceConnector = new Connector();
        sourceConnector.setType(ConnectorType.RocketMQ);
        sourceConnector.setAccessPoint(ROCKETMQ.getGrpcEndpoints());
        Connector sinkConnector = new Connector();
        sinkConnector.setType(ConnectorType.Kafka);
        sinkConnector.setProperties(kafkaProperties());
        SQLTransform<String, String> transform = new SQLTransform<>(SQL);
        RocketMQSource source = new RocketMQSource(sourceConnector, "group-" + suffix, List.of(inputTopic));
        KafkaSink sink = null;
        try {
            sink = new KafkaSink(sinkConnector, defaultTopic, "sink-" + suffix);
            startTask(new Task("rocketmq-kafka-" + suffix, source, transform, sink, 1));
        } catch (RuntimeException | Error failure) {
            try {
                source.close();
            } finally {
                if (sink != null) {
                    sink.close();
                }
            }
            throw failure;
        }

        rocketMqPublisher = ClientServiceProvider.loadService().newProducerBuilder()
            .setClientConfiguration(ClientConfiguration.newBuilder()
                .setEndpoints(ROCKETMQ.getGrpcEndpoints()).build())
            .setTopics(inputTopic).build();
        long beforePublish = System.currentTimeMillis();
        publishRocketMq(inputTopic, "cold", 5, null);
        SendReceipt defaultReceipt = publishRocketMq(inputTopic, "default", 25, null);
        SendReceipt routedReceipt = publishRocketMq(inputTopic, "routed", 30, routedTopic);

        List<ConsumerRecord<String, String>> records = receiveRecords(2);
        long afterReceive = System.currentTimeMillis();
        assertEquals(2, records.size(), "only the two SQL-matching inputs may reach Kafka");
        ConsumerRecord<String, String> defaultRecord = recordOnTopic(records, defaultTopic);
        ConsumerRecord<String, String> routedRecord = recordOnTopic(records, routedTopic);
        assertTransformed(defaultRecord, "default", 50);
        assertTransformed(routedRecord, "routed", 60);
        assertEquals(defaultReceipt.getMessageId().toString(), defaultRecord.key());
        assertEquals(routedReceipt.getMessageId().toString(), routedRecord.key());
        for (ConsumerRecord<String, String> record : records) {
            assertEquals(TimestampType.CREATE_TIME, record.timestampType());
            assertEquals(Long.parseLong(headerValue(record, "rmq.born.timestamp")), record.timestamp(),
                "RocketMQ born timestamp must survive SQL and Kafka publication");
            assertTrue(record.timestamp() >= beforePublish && record.timestamp() <= afterReceive);
            assertEquals(record.key(), headerValue(record, "rmq.message.id"));
            assertEquals(inputTopic, headerValue(record, "src.rmq.topic"));
            assertEquals("1", headerValue(record, Task.HOP_COUNT_HEADER));
            assertEquals("telemetry", headerValue(record, "rmq.tag"));
            assertEquals(Set.of("device-rmq", "sample"),
                Set.of(headerValue(record, "rmq.keys").split(",")));
            assertTrue(Integer.parseInt(headerValue(record, "rmq.delivery.attempt")) >= 1);
            assertFalse(headerValue(record, "rmq.born.host").isEmpty());
            assertEquals("rocketmq-integration", headerValue(record, "origin"));
            assertNull(record.headers().lastHeader("dst.kafka.topic"), "routing hint must be consumed");
        }
        assertNoMoreRecords();
    }

    private static String uniqueSuffix() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    private static Map<String, Object> kafkaProperties() {
        return Map.of("bootstrap.servers", KAFKA.getBootstrapServers(),
            "request.timeout.ms", 10_000, "delivery.timeout.ms", 30_000);
    }

    private void createKafkaVerifier(String... topics) throws Exception {
        AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()));
        try {
            List<NewTopic> newTopics = new ArrayList<>();
            for (String topic : topics) {
                newTopics.add(new NewTopic(topic, 1, (short) 1)
                    .configs(Map.of("message.timestamp.type", "CreateTime")));
            }
            admin.createTopics(newTopics).all().get(30, TimeUnit.SECONDS);
        } finally {
            admin.close(Duration.ofSeconds(10));
        }
        verifier = new KafkaConsumer<>(Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
            ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 30_000,
            ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000));
        List<TopicPartition> partitions = new ArrayList<>();
        for (String topic : topics) {
            partitions.add(new TopicPartition(topic, 0));
        }
        verifier.assign(partitions);
        verifier.seekToBeginning(partitions);
    }

    private void startTask(Task task) {
        taskThread = Thread.ofVirtual().name("task-" + task.getName()).start(() -> {
            try {
                task.launch();
                if (!shuttingDown.get()) {
                    taskFailure.set(new IllegalStateException("task exited before explicit shutdown"));
                }
            } catch (InterruptedException failure) {
                if (!shuttingDown.get()) {
                    taskFailure.set(failure);
                }
                Thread.currentThread().interrupt();
            } catch (Throwable failure) {
                // The RocketMQ client wraps an interrupted receive in RuntimeException.
                if (shuttingDown.get() && failure instanceof RuntimeException
                        && failure.getCause() instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                } else {
                    taskFailure.set(failure);
                }
            }
        });
    }

    private void publishMqtt(String topic, String device, int temperature, String messageId, String routedTopic) {
        var properties = Mqtt5UserProperties.builder()
            .add("origin", "mqtt-integration")
            .add("label", "blue").add("label", "green")
            .add("$__messageId", messageId).add("$__publisherClientId", "upstream-client")
            .add("src.mqtt.topic", "cannot-shadow-real-source");
        if (routedTopic != null) {
            properties.add("dst.kafka.topic", routedTopic);
        }
        mqttPublisher.publishWith().topic(topic).qos(MqttQos.AT_LEAST_ONCE)
            .payload(payload(device, temperature)).contentType("application/json")
            .responseTopic(topic + "/response").correlationData(ByteBuffer.wrap(CORRELATION_DATA))
            .userProperties(properties.build()).send();
    }

    private SendReceipt publishRocketMq(String topic, String device, int temperature, String routedTopic)
            throws Exception {
        MessageBuilder builder = ClientServiceProvider.loadService().newMessageBuilder()
            .setTopic(topic).setBody(payload(device, temperature))
            .setTag("telemetry").setKeys("device-rmq", "sample")
            .addProperty("origin", "rocketmq-integration");
        if (routedTopic != null) {
            builder.addProperty("dst.kafka.topic", routedTopic);
        }
        return rocketMqPublisher.send(builder.build());
    }

    private static byte[] payload(String device, int temperature) {
        return ("{\"device\":\"" + device + "\",\"temp\":" + temperature + "}")
            .getBytes(StandardCharsets.UTF_8);
    }

    private List<ConsumerRecord<String, String>> receiveRecords(int expectedCount) {
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        long deadline = System.nanoTime() + RECEIVE_TIMEOUT.toNanos();
        while (records.size() < expectedCount && System.nanoTime() < deadline) {
            assertTaskHealthy();
            verifier.poll(Duration.ofMillis(250)).forEach(records::add);
        }
        assertEquals(expectedCount, records.size(), "expected SQL-transformed records on Kafka topics");
        return records;
    }

    private void assertNoMoreRecords() {
        long deadline = System.nanoTime() + NEGATIVE_WINDOW.toNanos();
        while (System.nanoTime() < deadline) {
            assertTaskHealthy();
            assertTrue(verifier.poll(Duration.ofMillis(250)).isEmpty(),
                "filtered inputs and routed copies must not appear on either Kafka topic");
        }
    }

    private void assertTaskHealthy() {
        Throwable failure = taskFailure.get();
        if (failure != null) {
            fail("task lane terminated unexpectedly", failure);
        }
    }

    private static ConsumerRecord<String, String> recordOnTopic(
            List<ConsumerRecord<String, String>> records, String topic) {
        List<ConsumerRecord<String, String>> matching = records.stream()
            .filter(record -> record.topic().equals(topic)).toList();
        assertEquals(1, matching.size(), "expected exactly one matching record on " + topic);
        return matching.getFirst();
    }

    private static void assertTransformed(ConsumerRecord<String, String> record, String device, int doubled)
            throws Exception {
        assertEquals(MAPPER.valueToTree(Map.of("device", device, "doubled", doubled)),
            MAPPER.readTree(record.value()));
    }

    private static String headerValue(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        assertNotNull(header, "missing Kafka header " + name);
        assertNotNull(header.value(), "null Kafka header " + name);
        return new String(header.value(), StandardCharsets.UTF_8);
    }

    private static List<String> headerValues(ConsumerRecord<String, String> record, String name) {
        List<String> values = new ArrayList<>();
        for (Header header : record.headers().headers(name)) {
            values.add(new String(header.value(), StandardCharsets.UTF_8));
        }
        return values;
    }

    private static void createRocketMqTopic(String topic) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (true) {
            // Only internally-generated alphanumeric/hyphen topic names reach this shell command.
            ROCKETMQ.execInContainer("sh", "-c",
                "sh $ROCKETMQ_HOME/bin/mqadmin updateTopic -n 127.0.0.1:9876 -c DefaultCluster -t "
                    + topic + " >/dev/null 2>&1");
            String route = ROCKETMQ.execInContainer("sh", "-c",
                "sh $ROCKETMQ_HOME/bin/mqadmin topicRoute -n 127.0.0.1:9876 -t " + topic).getStdout();
            if (route.contains("brokerDatas")) {
                return;
            }
            if (System.nanoTime() >= deadline) {
                fail("topic route for " + topic + " not registered within 60s");
            }
            Thread.sleep(1_000);
        }
    }

    @AfterEach
    void tearDown() {
        shuttingDown.set(true);
        assertAll("stop lane and close all clients even if another cleanup fails",
            () -> {
                if (taskThread != null) {
                    taskThread.interrupt();
                    taskThread.join(15_000);
                    assertFalse(taskThread.isAlive(), "task thread survived interrupt");
                }
            },
            () -> {
                if (mqttPublisher != null) {
                    mqttPublisher.disconnect();
                }
            },
            () -> {
                if (rocketMqPublisher != null) {
                    rocketMqPublisher.close();
                }
            },
            () -> {
                if (verifier != null) {
                    verifier.close(Duration.ofSeconds(10));
                }
            },
            this::assertTaskHealthy);
    }
}
