package com.tencent.cloud.mqtt.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.tencent.cloud.mqtt.model.Connector;

public final class KafkaClients {
    private KafkaClients() {}

    public static Consumer<String, String> createConsumer(Connector connector, String clientId) {
        return new KafkaConsumer<>(consumerConfig(connector, clientId));
    }

    public static Map<String, Object> consumerConfig(Connector connector, String clientId) {
        Map<String, Object> properties = connector.getProperties();
        validateBootstrapServers(properties);
        for (String key : List.of(ConsumerConfig.GROUP_ID_CONFIG, ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)) {
            if (!(properties.get(key) instanceof String text) || text.isBlank()) {
                throw new IllegalArgumentException("Kafka source requires explicit non-blank string " + key);
            }
        }
        if (properties.containsKey(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG)) {
            throw new IllegalArgumentException("Kafka group.instance.id is not supported across source lanes");
        }
        Map<String, Object> config = copyNativeProperties(properties);
        for (String key : List.of(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)) {
            if (config.containsKey(key)) {
                throw new IllegalArgumentException("Kafka " + key + " is owned by the bridge (StringDeserializer)");
            }
        }
        config.putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.putIfAbsent(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        config.putIfAbsent(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        ConsumerConfig nativeConfig = new ConsumerConfig(config);
        if (nativeConfig.getBoolean(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)) {
            throw new IllegalArgumentException("Kafka enable.auto.commit=true is not supported: the source owns commits");
        }
        if (!"classic".equalsIgnoreCase(nativeConfig.getString(ConsumerConfig.GROUP_PROTOCOL_CONFIG))) {
            throw new IllegalArgumentException("Kafka group.protocol must be classic for this source");
        }
        if (!nativeConfig.getList(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG).isEmpty()) {
            throw new IllegalArgumentException("Kafka interceptor.classes is not supported by this source");
        }
        return config;
    }

    public static Producer<String, String> createProducer(Connector connector, String clientId) {
        return new KafkaProducer<>(producerConfig(connector, clientId));
    }

    public static Map<String, Object> producerConfig(Connector connector, String clientId) {
        Map<String, Object> properties = connector.getProperties();
        validateBootstrapServers(properties);
        Map<String, Object> config = copyNativeProperties(properties);
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

    private static Map<String, Object> copyNativeProperties(Map<String, Object> properties) {
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
            throw new IllegalArgumentException("Kafka bootstrap.servers must contain non-blank server addresses");
        }
    }
}
