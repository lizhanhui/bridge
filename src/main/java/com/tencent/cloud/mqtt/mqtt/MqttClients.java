package com.tencent.cloud.mqtt.mqtt;

import java.nio.charset.StandardCharsets;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5ClientBuilder;
import com.tencent.cloud.mqtt.model.Connector;

/** Builds hivemq MQTT5 clients from connector config. */
final class MqttClients {

    record HostPort(String host, int port, boolean ssl) {}

    private MqttClients() {}

    static HostPort parseAccessPoint(String accessPoint) {
        String host = accessPoint;
        int port = 1883;
        int colon = accessPoint.lastIndexOf(':');
        if (colon >= 0) {
            host = accessPoint.substring(0, colon);
            port = Integer.parseInt(accessPoint.substring(colon + 1));
        }
        return new HostPort(host, port, port == 8883);
    }

    static Mqtt5AsyncClient buildAsyncClient(Connector connector, String clientIdSuffix) {
        HostPort hp = parseAccessPoint(connector.getAccessPoint());
        Mqtt5ClientBuilder builder = MqttClient.builder()
            .useMqttVersion5()
            .identifier("bridge-" + connector.getId() + "-" + clientIdSuffix)
            .serverHost(hp.host())
            .serverPort(hp.port());
        if (hp.ssl()) {
            builder = builder.sslWithDefaultConfig();
        }
        builder = builder.simpleAuth()
            .username(connector.getUsername())
            .password(connector.getPassword().getBytes(StandardCharsets.UTF_8))
            .applySimpleAuth();
        return builder.buildAsync();
    }
}
