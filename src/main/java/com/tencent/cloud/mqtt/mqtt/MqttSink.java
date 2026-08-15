package com.tencent.cloud.mqtt.mqtt;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.tencent.cloud.mqtt.Sink;
import com.tencent.cloud.mqtt.model.Connector;

/** Publishes records to a fixed MQTT topic. Fail-fast: publish errors propagate and kill the task. */
public class MqttSink implements Sink {
    private static final Logger log = LoggerFactory.getLogger(MqttSink.class);

    private final Mqtt5AsyncClient client;
    private final String topic;
    private final String accessPoint;

    public MqttSink(Connector connector, String topic, String clientIdSuffix) {
        this.client = MqttClients.buildAsyncClient(connector, clientIdSuffix);
        this.topic = topic;
        this.accessPoint = connector.getAccessPoint();
        client.toBlocking().connect();
        log.info("MqttSink connected to {} for topic {}", accessPoint, topic);
    }

    @Override
    public void publish(Record<String, String> record) {
        try {
            client.toBlocking().publishWith()
                .topic(topic)
                .qos(MqttQos.AT_LEAST_ONCE)
                .payload(record.value().getBytes(StandardCharsets.UTF_8))
                .send();
        } catch (RuntimeException e) {
            log.error("Failed to publish to topic {} on {}", topic, accessPoint, e);
            throw e;
        }
    }

    @Override
    public void close() {
        client.toBlocking().disconnect();
    }
}
