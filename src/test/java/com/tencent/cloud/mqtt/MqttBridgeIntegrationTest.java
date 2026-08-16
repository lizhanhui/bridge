package com.tencent.cloud.mqtt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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

    private Connector connector() {
        Connector c = new Connector();
        c.setId("it");
        c.setType(ConnectorType.MQTT);
        c.setAccessPoint(HIVEMQ.getHost() + ":" + HIVEMQ.getMqttPort());
        return c;
    }

    /** Starts a real Task on a virtual thread. MqttSource subscribes in its constructor, so the subscription is live when this returns. */
    private void startTask(String name, String inFilter, String sql, String outTopic, int maxHops) {
        Task task = new Task(name,
            new MqttSource(connector(), inFilter, "source-" + name),
            new SQLTransform<>(sql),
            new MqttSink(connector(), outTopic, "sink-" + name),
            maxHops);
        taskThread = Thread.ofVirtual().start(() -> {
            try {
                task.launch();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    /** Verifier client subscribed to the given output topic; returns the publishes handle. */
    private Mqtt5BlockingClient.Mqtt5Publishes subscribeVerifier(String clientId, String outTopic) {
        verifier = MqttClient.builder()
            .useMqttVersion5()
            .identifier(clientId)
            .serverHost(HIVEMQ.getHost())
            .serverPort(HIVEMQ.getMqttPort())
            .buildBlocking();
        verifier.connect();
        Mqtt5BlockingClient.Mqtt5Publishes publishes = verifier.publishes(MqttGlobalPublishFilter.ALL);
        verifier.subscribeWith().topicFilter(outTopic).send();
        return publishes;
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

    @Test
    void transformsAndPublishesMatchingRecord() throws Exception {
        startTask("happy-0", "it/happy/in",
            "SELECT payload.device AS device, payload.temp * 2 AS doubled FROM payload WHERE payload.temp > 20",
            "it/happy/out", 1);
        Mqtt5BlockingClient.Mqtt5Publishes publishes = subscribeVerifier("verifier-happy", "it/happy/out");

        publish("it/happy/in", "{\"device\":\"d1\",\"temp\":25}",
            "origin", "it", "$__messageId", "msg-happy-1");

        Mqtt5Publish received = publishes.receive(RECEIVE_TIMEOUT_SECONDS, TimeUnit.SECONDS).orElse(null);
        assertNotNull(received, "expected transformed message on sink topic");
        assertEquals(
            MAPPER.readTree("{\"device\":\"d1\",\"doubled\":50}"),
            MAPPER.readTree(new String(received.getPayloadAsBytes(), StandardCharsets.UTF_8)));
        assertEquals(Optional.of("1"), userProperty(received, Task.HOP_COUNT_HEADER));
        assertEquals(Optional.of("it"), userProperty(received, "origin"));
    }
}
