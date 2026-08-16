package com.tencent.cloud.mqtt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.hivemq.HiveMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient.Mqtt5Publishes;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperties;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.tencent.cloud.mqtt.mqtt.MqttSink;
import com.tencent.cloud.mqtt.mqtt.MqttSource;
import com.tencent.cloud.mqtt.model.Connector;
import com.tencent.cloud.mqtt.model.ConnectorType;

@Testcontainers
@Timeout(60)
class MqttBridgeIntegrationTest {

    @Container
    static final HiveMQContainer HIVEMQ =
        new HiveMQContainer(DockerImageName.parse("hivemq/hivemq-ce:2026.5"));

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final long RECEIVE_TIMEOUT_SECONDS = 10;
    private static final long NEGATIVE_WINDOW_MILLIS = 3_000;

    private Thread taskThread;
    private Mqtt5BlockingClient verifier;
    private Mqtt5Publishes verifierPublishes;

    private Connector connector() {
        Connector c = new Connector();
        c.setId("it");
        c.setType(ConnectorType.MQTT);
        c.setAccessPoint(HIVEMQ.getHost() + ":" + HIVEMQ.getMqttPort());
        return c;
    }

    /** Starts a real Task on a virtual thread. MqttSource subscribes in its constructor, so the subscription is live when this returns. */
    private void startTask(String name, String inFilter, String sql, String outTopic, int maxHops) {
        MqttSource source = new MqttSource(connector(), inFilter, "source-" + name);
        Task task;
        try {
            task = new Task(name,
                source,
                new SQLTransform<>(sql),
                new MqttSink(connector(), outTopic, "sink-" + name),
                maxHops);
        } catch (RuntimeException e) {
            source.close();
            throw e;
        }
        taskThread = Thread.ofVirtual().start(() -> {
            try {
                task.launch();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    /** Verifier client subscribed to the given output topic; returns the publishes handle (closed by tearDown). */
    private Mqtt5Publishes subscribeVerifier(String clientId, String outTopic) {
        verifier = MqttClient.builder()
            .useMqttVersion5()
            .identifier(clientId)
            .serverHost(HIVEMQ.getHost())
            .serverPort(HIVEMQ.getMqttPort())
            .buildBlocking();
        verifier.connect();
        verifierPublishes = verifier.publishes(MqttGlobalPublishFilter.ALL);
        verifier.subscribeWith().topicFilter(outTopic).send();
        return verifierPublishes;
    }

    private void publish(String topic, String payload, String... userProperties) {
        var builder = verifier.publishWith().topic(topic)
            .payload(payload.getBytes(StandardCharsets.UTF_8));
        if (userProperties.length > 0) {
            var props = Mqtt5UserProperties.builder();
            for (int i = 0; i < userProperties.length; i += 2) {
                props.add(userProperties[i], userProperties[i + 1]);
            }
            builder.userProperties(props.build());
        }
        builder.send();
    }

    private static Optional<String> userProperty(Mqtt5Publish publish, String name) {
        return publish.getUserProperties().asList().stream()
            .filter(p -> p.getName().toString().equals(name))
            .map(p -> p.getValue().toString())
            .findFirst();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (taskThread != null) {
            taskThread.interrupt();
            taskThread.join(10_000);
            assertFalse(taskThread.isAlive(), "task thread survived interrupt");
        }
        if (verifierPublishes != null) {
            verifierPublishes.close();
        }
        if (verifier != null) {
            verifier.disconnect();
        }
    }

    @Test
    void brokerRoundTrip() throws Exception {
        Mqtt5BlockingClient client = MqttClient.builder()
            .useMqttVersion5()
            .identifier("smoke")
            .serverHost(HIVEMQ.getHost())
            .serverPort(HIVEMQ.getMqttPort())
            .buildBlocking();
        client.connect();
        try (Mqtt5Publishes publishes = client.publishes(MqttGlobalPublishFilter.ALL)) {
            client.subscribeWith().topicFilter("it/smoke").send();
            client.publishWith().topic("it/smoke")
                .payload("ping".getBytes(StandardCharsets.UTF_8)).send();

            Mqtt5Publish received = publishes.receive(10, TimeUnit.SECONDS).orElse(null);

            assertNotNull(received);
            assertEquals("ping", new String(received.getPayloadAsBytes(), StandardCharsets.UTF_8));
        } finally {
            client.disconnect();
        }
    }

    /** Asserts nothing arrives on the output topic within the negative window. */
    private void assertNoMessage(Mqtt5Publishes publishes) throws Exception {
        assertTrue(publishes.receive(NEGATIVE_WINDOW_MILLIS, TimeUnit.MILLISECONDS).isEmpty(),
            "unexpected message on sink topic");
    }

    @Test
    void transformsAndPublishesMatchingRecord() throws Exception {
        startTask("happy-0", "it/happy/in",
            "SELECT payload.device AS device, payload.temp * 2 AS doubled FROM payload WHERE payload.temp > 20",
            "it/happy/out", 1);
        Mqtt5Publishes publishes = subscribeVerifier("verifier-happy", "it/happy/out");

        publish("it/happy/in", "{\"device\":\"d1\",\"temp\":25}",
            "origin", "it", "$__messageId", "msg-happy-1");

        Mqtt5Publish received = publishes.receive(RECEIVE_TIMEOUT_SECONDS, TimeUnit.SECONDS).orElse(null);
        assertNotNull(received, "expected transformed message on sink topic");
        assertEquals("it/happy/out", received.getTopic().toString());
        assertEquals(
            MAPPER.readTree("{\"device\":\"d1\",\"doubled\":50}"),
            MAPPER.readTree(new String(received.getPayloadAsBytes(), StandardCharsets.UTF_8)));
        assertEquals(Optional.of("1"), userProperty(received, Task.HOP_COUNT_HEADER));
        assertEquals(Optional.of("it"), userProperty(received, "origin"));
    }

    @Test
    void dropsRecordFilteredOutBySql() throws Exception {
        startTask("filter-0", "it/filter/in",
            "SELECT * FROM payload WHERE payload.temp > 20",
            "it/filter/out", 1);
        Mqtt5Publishes publishes = subscribeVerifier("verifier-filter", "it/filter/out");

        publish("it/filter/in", "{\"device\":\"cold\",\"temp\":5}");
        publish("it/filter/in", "{\"device\":\"hot\",\"temp\":25}");

        Mqtt5Publish received = publishes.receive(RECEIVE_TIMEOUT_SECONDS, TimeUnit.SECONDS).orElse(null);
        assertNotNull(received, "expected the matching message");
        String body = new String(received.getPayloadAsBytes(), StandardCharsets.UTF_8);
        assertTrue(body.contains("hot"), "only the matching record should be republished, got: " + body);
        assertNull(publishes.receive(NEGATIVE_WINDOW_MILLIS, TimeUnit.MILLISECONDS).orElse(null),
            "filtered record must not reach the sink");
    }

    @Test
    void dropsRecordExceedingMaxHops() throws Exception {
        startTask("loop-0", "it/loop/in",
            "SELECT * FROM payload",
            "it/loop/out", 1);
        Mqtt5Publishes publishes = subscribeVerifier("verifier-loop", "it/loop/out");

        publish("it/loop/in", "{\"marker\":\"over-limit\"}",
            Task.HOP_COUNT_HEADER, "2");
        publish("it/loop/in", "{\"marker\":\"fresh\"}");

        Mqtt5Publish received = publishes.receive(RECEIVE_TIMEOUT_SECONDS, TimeUnit.SECONDS).orElse(null);
        assertNotNull(received, "expected the fresh message");
        String body = new String(received.getPayloadAsBytes(), StandardCharsets.UTF_8);
        assertTrue(body.contains("fresh"), "over-limit record must be dropped, got: " + body);
        assertEquals(Optional.of("1"), userProperty(received, Task.HOP_COUNT_HEADER));
        assertNull(publishes.receive(NEGATIVE_WINDOW_MILLIS, TimeUnit.MILLISECONDS).orElse(null),
            "over-limit record must not be republished");
    }
}
