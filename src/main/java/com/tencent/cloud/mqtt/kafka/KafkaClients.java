package com.tencent.cloud.mqtt.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import com.tencent.cloud.mqtt.model.Connector;

public final class KafkaClients {
    private KafkaClients() {}

    public static Producer<String, String> createProducer(Connector connector, String clientId) {
        return new KafkaProducer<>(producerConfig(connector, clientId));
    }

    public static Map<String, Object> producerConfig(Connector connector, String clientId) {
        Map<String, Object> properties = connector.getProperties();
        validateBootstrapServers(properties);
        Map<String, Object> config = new HashMap<>();
        properties.forEach((key, value) -> {
            if (value instanceof List<?> list) {
                if (list.stream().anyMatch(item -> !(item instanceof String))) {
                    throw new IllegalArgumentException("Kafka property " + key + " must be a string list");
                }
                config.put(key, new ArrayList<>(list));
            } else if (value instanceof String || value instanceof Number || value instanceof Boolean) {
                config.put(key, value);
            } else {
                throw new IllegalArgumentException("Kafka property " + key + " must be a scalar or string list");
            }
        });
        for (String key : List.of(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)) {
            if (config.containsKey(key)) {
                throw new IllegalArgumentException("Kafka " + key + " is owned by the bridge (StringSerializer)");
            }
        }
        if (config.containsKey(ProducerConfig.TRANSACTIONAL_ID_CONFIG)) {
            throw new IllegalArgumentException("Kafka transactional.id is not supported by this sink");
        }
        config.putIfAbsent(ProducerConfig.ACKS_CONFIG, "all");
        config.putIfAbsent(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.putIfAbsent(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        if ("0".equals(config.get(ProducerConfig.ACKS_CONFIG).toString().trim())) {
            throw new IllegalArgumentException("Kafka acks=0 is not supported: publication requires acknowledgement");
        }
        // Native validation includes explicitly-enabled idempotence conflicts.
        new ProducerConfig(config);
        return config;
    }

    public static void validateBootstrapServers(Map<String, Object> properties) {
        Object bootstrap = properties == null ? null : properties.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
        List<?> servers;
        if (bootstrap instanceof String text) {
            servers = List.of(text.split(",", -1));
        } else if (bootstrap instanceof List<?> list) {
            servers = list;
        } else {
            throw new IllegalArgumentException("Kafka properties require bootstrap.servers as a string or string list");
        }
        if (servers.isEmpty() || servers.stream().anyMatch(server -> !(server instanceof String text) || text.isBlank())) {
            throw new IllegalArgumentException("Kafka bootstrap.servers must contain nonblank server addresses");
        }
    }
}
