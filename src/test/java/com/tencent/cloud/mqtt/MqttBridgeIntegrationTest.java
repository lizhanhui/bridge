package com.tencent.cloud.mqtt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.testcontainers.hivemq.HiveMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient.Mqtt5Publishes;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;

@Testcontainers
class MqttBridgeIntegrationTest {

    @Container
    static final HiveMQContainer HIVEMQ =
        new HiveMQContainer(DockerImageName.parse("hivemq/hivemq-ce:2026.5"));

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
}
