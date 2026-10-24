package com.tencent.cloud.mqtt.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.testcontainers.kafka.KafkaContainer;

public final class AuthenticatedKafkaContainer extends KafkaContainer {
    private final String username = "u" + UUID.randomUUID().toString().replace("-", "");
    private final String password = "p" + UUID.randomUUID().toString().replace("-", "");

    public AuthenticatedKafkaContainer() {
        super("apache/kafka:3.7.0");
        // KafkaContainer advertises the PLAINTEXT listener name; only its external protocol changes.
        withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
            "BROKER:PLAINTEXT,PLAINTEXT:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT");
        withEnv("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN");
        withEnv("KAFKA_LISTENER_NAME_PLAINTEXT_SASL_ENABLED_MECHANISMS", "PLAIN");
        withEnv("KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG",
            "org.apache.kafka.common.security.plain.PlainLoginModule required user_"
                + username + "=\"" + password + "\";");
    }

    public Map<String, Object> clientProperties() {
        return new HashMap<>(Map.of(
            "bootstrap.servers", getBootstrapServers(),
            "security.protocol", "SASL_PLAINTEXT",
            "sasl.mechanism", "PLAIN",
            "sasl.jaas.config", clientLogin(password)));
    }

    public Map<String, Object> clientProperties(Map<String, Object> overrides) {
        Map<String, Object> properties = clientProperties();
        properties.putAll(overrides);
        return properties;
    }

    public Map<String, Object> badPasswordProperties() {
        Map<String, Object> properties = clientProperties();
        properties.put("sasl.jaas.config", clientLogin("invalid" + UUID.randomUUID().toString().replace("-", "")));
        return properties;
    }

    private String clientLogin(String candidatePassword) {
        return "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
            + username + "\" password=\"" + candidatePassword + "\";";
    }
}
