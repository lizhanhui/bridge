package com.tencent.cloud.mqtt.mqtt;

import java.nio.charset.StandardCharsets;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5ClientBuilder;
import com.tencent.cloud.mqtt.model.Connector;

/** Builds hivemq MQTT5 clients from connector config. */
final class MqttClients {

    private static final int DEFAULT_PORT = 1883;
    private static final int TLS_PORT = 8883;

    record HostPort(String host, int port, boolean ssl) {}

    private MqttClients() {}

    /** Parses an access point of the form {@code host[:port]} (no scheme, no bare IPv6). */
    static HostPort parseAccessPoint(String accessPoint) {
        String host = accessPoint;
        int port = DEFAULT_PORT;
        int colon = accessPoint.lastIndexOf(':');
        if (colon >= 0) {
            host = accessPoint.substring(0, colon);
            try {
                port = Integer.parseInt(accessPoint.substring(colon + 1));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid access point: " + accessPoint, e);
            }
        }
        return new HostPort(host, port, port == TLS_PORT);
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
        String username = connector.getUsername();
        if (username != null && !username.isBlank()) {
            builder = builder.simpleAuth()
                .username(username)
                .password(connector.getPassword().getBytes(StandardCharsets.UTF_8))
                .applySimpleAuth();
        }
        return builder.buildAsync();
    }
}
